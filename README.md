Queuefka
===
A kafka inspired embedded persistent Apend Only Log message queue.

## Overview

Need to get data from your ephemeral/cloud/embedded platform onto your server? Challenged by rebooting hardware, irregular or poor network connectivity, low memory footprint? Stick a queuefka between the producer and the publisher.  Data messages will pile up on disk until they can be sent off to the server.

*Warning:* Currently queufka is *very untested* and *very alpha* software and quite brittle in the face of actual disk corruption.

## Usage

    wt, _ := queuefka.NewWriter("./mytopic", 1024 * 1024)
    wt.Write([]byte(string("こんにちは世界!")))
    wt.Flush()

    rd, _ := queuefka.NewReader("./mytopic", 0x0000)
    msg, _ := rd.Read()
    println(string(msg))

## Benchmark

    cd $GOPATH
    go get github.com/syndtr/goleveldb/leveldb
    go get github.com/boltdb/bolt/...
    go get github.com/vova616/xxhash
    go get github.com/ubergarm/queuefka
    go test github.com/ubergarm/queuefka -bench=".*"
    rm -rf /tmp/my* # to cleanup

## Benchmark Results

CPU: Intel(R) Core(TM)2 Duo CPU     P8600  @ 2.40GHz

DISK: ~54MiB/s

    $ time (dd if=/dev/zero of=/tmp/mytest.file bs=1MB count=1024 && sync)
    real    0m18.962s
    user    0m0.016s
    sys     0m1.676s

    $ go test github.com/ubergarm/queuefka -bench=".*"
    Benchmark_Leveldb_Put-2           100000             13752 ns/op
    Benchmark_Boltdb_Put-2            300000              4923 ns/op
    Benchmark_Os_Write-2              300000              5125 ns/op
    Benchmark_Bufio_Write-2         20000000               106 ns/op
    Benchmark_Queuefka_Write-2       5000000               322 ns/op
    Benchmark_Queuefka_Read-2        5000000               316 ns/op

## On Disk Format

Each log entry payload is written along with the following header:

    Fixed Header Size: 64 bits

    message length: 4 byte uint32, little endian, (in bytes, 4 + n)
    crc           : 4 byte uint32, little endian, xxhash
    payload:      : n bytes


Compare to kafka:

    message length : 4 bytes (value: 1+4+n)
    "magic" value  : 1 byte
    crc            : 4 bytes
    payload        : n bytes

## Design

A queufka.NewWriter() creates new (or loads an existing):

* a single topic in the specified path on on disk
* each topic is comprised an a single implicit partition
* multiple segment (slab) files per partition
* 64 bit topic address allows up to an Exabyte of data per topic
* 32 bit message addres allows up to 4GiB per individual message

Consistency is maintained using xxhash.  It currently uses some unsafe code but is fast.

While a CRC can detect errors in the message payload, missing or corrupted data in the header, especially the size, will wreak havoc. A more complex header framing including header crcs and magic sequences of bytes etc could help improve durability.

## Dependencies

* [vova616/xxhash](https://github.com/vova616/xxhash)

## TODO

* Address all TODO and FIXME in code comments
* Error Checking
  * Implement and use more error codes
  * Better bounds checking during read/writes
  * Error recovery if possible
* Test
  * Large Message Payloads
  * Large Topics
  * Various backing file systems (EXT4/XFS/Snapshots/Compression/dm-crypt)
* Examples
  * disk backed channel
  * kafka client
  * curl put / get reverse-proxy-able microservice
  * cron job to cleanup stale slabs after 7 days
  * Flush() after N writes or Y seconds
* Refactor
  * Make code more GO idiomatic
  * Build / test for concurrency e.g. lock files or ???
  * Mirror bufio api more closely
* Dockerfile
* Travis build automation
* Documentation

## References

* [Jay Kreps - The Log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
* [apache kafka](https://kafka.apache.org/documentation.html#log)
* [jedisct1/flowgger](https://github.com/jedisct1/flowgger)
* [bogdanovich/siberite](https://github.com/bogdanovich/siberite)
* [sclasen/event-shuttle](https://github.com/sclasen/event-shuttle)
* [redis persistence](http://redis.io/topics/persistence)
* [ledisdb.com](http://ledisdb.com/)
* [lmdb](http://symas.com/mdb/)
* [go serialization benchmarks](https://github.com/alecthomas/go_serialization_benchmarks)

## Contributing

This is my first bit of GO code. Feel free to pitch in or implement your own super fast zero-copy everything I wish this were `queuefka` in a different language and post benchmarks or it didn't happen! ;)
