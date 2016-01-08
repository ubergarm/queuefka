// Copyright (c) 2015-2016 John W. Leimgruber III <blog.ubergarm.com>
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package queuefka implements Append Only Log functionality.  It wraps a
// bufio.Reader or bufio.Writer object, creating another object (Reader or
// Writer) that also implements the interface but handles stream framing,
// CRCs, and segment file management.

package queuefka

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/vova616/xxhash"
)

var (
	ErrInvalidTopic = errors.New("queuefka: Read() invalid topic path")
	ErrEndOfLog     = errors.New("queuefka: Read() end of log")
	ErrOutOfBounds  = errors.New("queuefka: Read() topic address out of bounds")
	ErrBadChecksum  = errors.New("queuefka: Read() checksum mismatch")
)

// Reader implements Append Only Log functionality for an bufio.Reader object.
type Reader struct {
	topic string // path to directory which holds *.slab files
	base  uint64 // address of first message in current slab file e.g. <base>.slab
	fp    *os.File
	rd    *bufio.Reader
}

// Seek sets up Reader file pointer, bufio reader, for a given absoulute log address
func (rd *Reader) Seek(topic string, address uint64) error {
	// close any existing file pointer
	if rd.fp != nil {
		rd.fp.Close()
	}

	slabs := SlabFiles(rd.topic)

	// error if there are no .slab files found
	if len(slabs) <= 0 {
		return ErrInvalidTopic
	}

	// sequentially search through all slab files until one contains offset
	// assumes fixed style slab file name e.g. "< 20 characters >.slab"
	slabFile := slabs[0]
	for i := 0; i < len(slabs); i++ {
		basename := slabs[i][(len(slabs[i]) - 25):(len(slabs[i]) - 5)]
		d, _ := strconv.Atoi(basename)
		if address < uint64(d) {
			break
		}
		slabFile = slabs[i]
		rd.base = uint64(d)
	}

	// open file
	fp, err := os.OpenFile(slabFile, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	rd.fp = fp

	// check out of bounds
	stat, _ := rd.fp.Stat()
	if (address - rd.base) > uint64(stat.Size()) {
		return ErrOutOfBounds
	}

	// check if end of log
	if (address - rd.base) == uint64(stat.Size()) {
		// new buffered reader at begginning of fp
		rd.rd = bufio.NewReader(rd.fp)
		return ErrEndOfLog
	}

	// seek file cursor to offset
	offset := int64(rd.base - address)
	_, err = rd.fp.Seek(offset, os.SEEK_SET)
	if err != nil {
		return err
	}

	// new buffered reader at the cursor location of fp
	rd.rd = bufio.NewReader(rd.fp)

	return nil
}

// NewReader returns a new Reader starting at the specified topic and address
func NewReader(topic string, address uint64) (*Reader, error) {
	rd := &Reader{topic: topic}

	err := rd.Seek(topic, address)
	if err != nil {
		return rd, err
	}

	return rd, nil
}

// TODO: possibly optimize by having caller pass in a buffer reference?
//       also need to give user the address so they can keep track of it
// returns single messages sequentially
func (rd *Reader) Read() ([]byte, error) {
	var dlen, xx32 uint32
	buf := make([]byte, 4)

	// read 4 bytes length
	for cnt := 0; cnt < 4; {
		rx, err := rd.rd.Read(buf[cnt:])
		if err == io.EOF {
			offset, _ := rd.fp.Seek(0, os.SEEK_CUR)
			//TODO test this reader changing slab file code, seems brittle
			// issues with reader outpacing writer?? file locks? ugh?
			rd.base += uint64(offset)
			err := rd.Seek(rd.topic, rd.base)
			if err != nil {
				return nil, err
			}
			continue
		} else if err != nil {
			return nil, err
		}
		cnt += rx
	}
	dlen = binary.LittleEndian.Uint32(buf)

	// read 4 bytes crc
	for cnt := 0; cnt < 4; {
		rx, err := rd.rd.Read(buf[cnt:])
		if err != nil {
			return nil, err
		}
		cnt += rx
	}
	xx32 = binary.LittleEndian.Uint32(buf)

	// read data payload
	buf = make([]byte, dlen)
	for cnt := 0; uint32(cnt) < dlen; {
		rx, err := rd.rd.Read(buf[cnt:])
		if err != nil {
			return nil, err
		}
		cnt += rx
	}

	// check crc
	if xx32 != xxhash.Checksum32(buf) {
		return buf, ErrBadChecksum
	}

	return buf, nil
}

// cleanup Reader
func (rd *Reader) Close() error {
	return rd.fp.Close()
}

// Writer implements Append Only Log functionality for a bufio.Writer object.
type Writer struct {
	topic        string   // path to directory which holds *.slab files
	address      uint64   // absolute address of whole log in bytes
	base         uint64   // absolute offset of current slab file e.g. <base>.slab
	fp           *os.File // file pointer for writing to log address
	wt           *bufio.Writer
	slabSizeHint uint64 // once a slab exceeds this size roll a fresh one
	sync.Mutex          // mutex to lock while writing to log address
}

// return names of all slab files present in wt.topic
func SlabFiles(topic string) []string {
	files, err := filepath.Glob(topic + "/*.slab")
	if err != nil {
		log.Panic(err)
	}
	return files
}

// load and validate *.slab files from wt.topic
func (wt *Writer) load() {
	files, err := filepath.Glob(wt.topic + "/*.slab")
	if err != nil {
		log.Panic(err)
	}

	latest := files[len(files)-1]

	// open slab file with highest log address in name
	fp, err := os.OpenFile(latest, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		log.Panic(err)
	}

	// the absolute address is (biggest segment name + biggest segment size)
	stat, _ := fp.Stat()
	i, _ := strconv.Atoi(stat.Name()[:len(stat.Name())-5])
	wt.base = uint64(i)
	wt.address = wt.base + uint64(stat.Size())
	wt.fp = fp
	wt.wt = bufio.NewWriter(wt.fp)
	wt.Flush()
}

// create a new log slab in wt.topic
func (wt *Writer) create() error {
	// create topic if necessary
	err := os.MkdirAll(wt.topic, 0700)
	if err != nil {
		return err
	}

	// create a new slab file
	fname := fmt.Sprintf("%s/%020d.slab", wt.topic, wt.address)
	wt.base = wt.address

	fp, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	// TODO trunc or hints depending on size to prealloc ext4/xfs etc?
	// could possibly optimize this here for sequential writes etc...
	// Don't truncate for now as it confuses finding address on a new file
	// fp.Truncate(int64(wt.slabSizeHint))
	wt.fp = fp
	wt.wt = bufio.NewWriter(wt.fp)
	wt.Flush()

	return nil
}

// NewWriter returns a Writer after creating a topic or seeking address properly
func NewWriter(topic string, slabSizeHint uint64) (*Writer, error) {
	var wt *Writer
	wt = &Writer{slabSizeHint: slabSizeHint}

	wt.topic = topic

	if len(SlabFiles(wt.topic)) == 0 {
		// create a new topic
		wt.create()
	} else {
		// load existing topic with cursor at the end of the highest address file
		wt.load()
	}

	return wt, nil
}

func (wt *Writer) Close() error {
	wt.Flush()
	return wt.fp.Close()
}

func (wt *Writer) Write(d []byte) error {
	var dlen, xx32 uint32
	buf := make([]byte, 4)

	dlen = uint32(len(d))
	xx32 = xxhash.Checksum32(d)

	wt.Lock()

	// FIXME -- make a function like WriteAll() to write until all written
	// e.g.
	// for cnt = 0; cnt < len(key); {
	//     tx, _ := fp.Write(key[cnt:])
	//     cnt += tx
	// }

	// write header
	binary.LittleEndian.PutUint32(buf, dlen)
	tx, err := wt.wt.Write(buf)
	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(buf, xx32)
	tx, err = wt.wt.Write(buf)
	if err != nil {
		return err
	}

	// write payload
	tx, err = wt.wt.Write(d)
	if err != nil {
		return err
	}

	// update address
	wt.address = wt.address + uint64(8+tx)

	// roll over slab file if it is big enough
	if (wt.address - wt.base) > wt.slabSizeHint {
		wt.Flush()
		wt.fp.Close()
		wt.create()
	}

	wt.Unlock()

	return nil
}

func (wt *Writer) Flush() error {
	return wt.wt.Flush()
}

func (wt *Writer) Status() {
	stat, _ := wt.fp.Stat()
	log.Printf("===================================================\n")
	log.Printf("Queuefka Log Status\n")
	log.Printf("    absolute address : %d\n", wt.address)
	log.Printf("    no of segments   : %d\n", len(SlabFiles(wt.topic)))
	log.Printf("    total size       : %.1fMB\n", float32(wt.address/1024.0/1024.0))
	log.Printf("    log directory    : %s\n", wt.topic)
	log.Printf("    current segment  : %s\n", stat.Name())
	log.Printf("    segment size     : %.1fMB\n", float32((stat.Size() / 1024.0 / 1024.0)))
	log.Printf("===================================================\n")
}
