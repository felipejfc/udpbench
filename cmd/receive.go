/*
 * Copyright (c) 2020 Felipe Cavalcanti <fjfcavalcanti@gmail.com> Author: Felipe
 * Cavalcanti <fjfcavalcanti@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package cmd

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var addr string
var bytesRecv int64
var msgsRecv int64
var bufferPool sync.Pool
var shouldReply bool

func handleUDPConnection(conn *net.UDPConn) {
	buffer := bufferPool.Get().([]byte)
	n, addr, err := conn.ReadFromUDP(buffer)

	if err != nil {
		log.Fatal(err)
	}

	atomic.AddInt64(&bytesRecv, int64(n))
	atomic.AddInt64(&msgsRecv, 1)

	if shouldReply {
		_, err = conn.WriteToUDP(buffer, addr)

		if err != nil {
			log.Println(err)
		}
	}

	bufferPool.Put(buffer)
}

func printRecvStats() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			log.Printf("receiving rate: %d msg/s | %d bytes/s\n", msgsRecv, bytesRecv)
			atomic.SwapInt64(&msgsRecv, 0)
			atomic.SwapInt64(&bytesRecv, 0)
		}
	}
}

var receiveCmd = &cobra.Command{
	Use:   "receive",
	Short: "starts UDP bench in receive mode",
	Long:  `starts UDP bench in receive mode`,
	Run: func(cmd *cobra.Command, args []string) {
		bufferPool = sync.Pool{New: func() interface{} { return make([]byte, 2048) }}
		log.Printf("starting udp bench in receiver mode on %s\n", addr)
		udpAddr, err := net.ResolveUDPAddr("udp4", addr)

		if err != nil {
			log.Fatal(err)
		}

		ln, err := net.ListenUDP("udp", udpAddr)

		if err != nil {
			log.Fatal(err)
		}

		defer ln.Close()

		go printRecvStats() // TODO can be one with the one in send

		for {
			// wait for UDP client to connect
			handleUDPConnection(ln)
		}
	},
}

func init() {
	receiveCmd.Flags().StringVarP(&addr, "address", "a", "localhost:5000", "the address to listen on")
	receiveCmd.Flags().BoolVarP(&shouldReply, "shouldReply", "r", false, "whether we should reply the received messages")
	RootCmd.AddCommand(receiveCmd)
}
