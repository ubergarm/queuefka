// Copyright (c) 2015-2016 John W. Leimgruber III <blog.ubergarm.com>
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package main

import "github.com/ubergarm/queuefka"

func main() {
	wt, _ := queuefka.NewWriter("./mytopic", 1024*1024)

	defer wt.Close()
	wt.Write([]byte(string("こんにちは世界!"))) // Hello World in Japanese (i hope)
	wt.Flush()

	rd, _ := queuefka.NewReader("./mytopic", 0x0000)
	defer rd.Close()
	msg, _ := rd.Read()
	println(string(msg))
}
