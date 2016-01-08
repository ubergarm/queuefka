// Copyright (c) 2015-2016 John W. Leimgruber III <blog.ubergarm.com>
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package queuefka_test

import (
	"bufio"
	"encoding/binary"
	"os"
	"testing"

	"github.com/ubergarm/queuefka"

	"github.com/boltdb/bolt"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	value = []byte("This is only a test.") // test message payload
	size  = 20                             // number of bytes in payload
	hash  = 0x8e217fe1                     // xxhash

	segmentSizeHint = uint64(1024 * 1024 * 10)  // 10MiB
	topic           = string("/tmp/mylog")      // for queuefka log
	rawTopic        = string("/tmp/mylog.raw")  // for direct file i/o
	myBoltDB        = string("/tmp/mybolt.db")  // for bolt db
	myLevelDB       = string("/tmp/mylevel.db") // for level db
)

func Test_Queuefka(t *testing.T) {
	wt, err := queuefka.NewWriter(topic, segmentSizeHint)
	if err != nil {
		panic(err)
	}
	defer wt.Close()

	rd, err := queuefka.NewReader(topic, 0x0000)
	if err != nil && err != queuefka.ErrEndOfLog {
		panic(err)
	}
	defer rd.Close()

	wt.Write(value)
	wt.Flush()

	raw, err := rd.Read()
	if err != nil && err != queuefka.ErrEndOfLog {
		panic(err)
	}

	msg := string(raw)
	if msg != string(value) {
		println(msg)
		panic("queuefka: Read does not match write:")
	}

	wt.Status()
}

func Test_Queuefka_Sequential(t *testing.T) {
	wt, err := queuefka.NewWriter(topic, segmentSizeHint)
	if err != nil {
		panic(err)
	}
	defer wt.Close()

	rd, err := queuefka.NewReader(topic, 0x0000)
	if err != nil && err != queuefka.ErrEndOfLog {
		panic(err)
	}
	defer rd.Close()

	for i := 0; i < 50000; i++ {
		wt.Write(value)
		wt.Flush()

		raw, err := rd.Read()
		if err != nil {
			if err == queuefka.ErrEndOfLog {
				break
			}
			panic(err)
		}

		msg := string(raw)
		if msg != string(value) {
			println(msg)
			panic("queuefka: Read does not match write:")
		}

		// this extra read should return ErrEndOfLog
		raw, err = rd.Read()
		if err != nil {
			if err != queuefka.ErrEndOfLog {
				panic(err)
			}
			continue
		}

	}

	wt.Status()
}

func Benchmark_Leveldb_Put(b *testing.B) {
	key := make([]byte, 8)
	db, _ := leveldb.OpenFile(myLevelDB, nil)
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key, uint64(i))
		db.Put(key, value, nil)
	}
	db.Close()
}

func Benchmark_Boltdb_Put(b *testing.B) {
	key := make([]byte, 8)
	var world = []byte("world")
	db, _ := bolt.Open(myBoltDB, 0600, nil)
	_ = db.Update(func(tx *bolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists(world)

		for i := 0; i < b.N; i++ {
			binary.LittleEndian.PutUint64(key, uint64(i))
			_ = bucket.Put(key, value)
		}

		return nil
	})
	db.Close()
}

func Benchmark_Os_Write(b *testing.B) {
	key := make([]byte, 8)
	fp, _ := os.OpenFile(rawTopic, os.O_CREATE|os.O_RDWR, 0600)
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key, uint64(i))
		fp.Write(key)
		fp.Write(value)
	}
	fp.Close()
}

func Benchmark_Bufio_Write(b *testing.B) {
	key := make([]byte, 8)
	fp, _ := os.OpenFile(rawTopic, os.O_CREATE|os.O_RDWR, 0600)
	w := bufio.NewWriter(fp)
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key, uint64(i))
		w.Write(key)
		w.Write(value)
	}
	w.Flush()
	fp.Close()
}

func Benchmark_Queuefka_Write(b *testing.B) {
	wt, _ := queuefka.NewWriter(topic, segmentSizeHint)
	for i := 0; i < b.N; i++ {
		wt.Write(value)
	}
	wt.Close()
}

func Benchmark_Queuefka_Read(b *testing.B) {
	rd, _ := queuefka.NewReader(topic, 0x0000)
	for i := 0; i < b.N; i++ {
		_, err := rd.Read()
		if err != nil {
			if err == queuefka.ErrEndOfLog {
				println("Not enough data in queuefka log to test fully benchmark Read()")
				break
			}
			panic(err)
		}
	}
	rd.Close()
}
