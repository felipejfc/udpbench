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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

var numClients int
var target string
var rate int
var packetSize int
var sent int64
var done chan (os.Signal)
var answersRecv int64
var shouldListenForAnswers bool

func listenAnswers(conn *net.UDPConn) {
	for {
		buffer := bufferPool.Get().([]byte)
		_, _, err := conn.ReadFrom(buffer)
		if err != nil {
			log.Println(err)
		} else {
			atomic.AddInt64(&answersRecv, 1)
		}
		bufferPool.Put(buffer)
	}
}

func send() {
	message := make([]byte, packetSize)
	RemoteAddr, err := net.ResolveUDPAddr("udp", target)
	conn, err := net.DialUDP("udp", nil, RemoteAddr)

	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	go listenAnswers(conn)

	ticker := time.NewTicker(time.Duration((1/float64(rate))*1000) * time.Millisecond)
	for {
		select {
		case <-done:
			log.Println("exiting send routine...")
			return
		case <-ticker.C:
			_, err := conn.Write(message)
			if err != nil {
				log.Printf("error sending message: %s\n", err.Error())
			} else {
				atomic.AddInt64(&sent, 1)
			}
		}
	}
}

func printStats() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			log.Printf("sending rate: %d msg/s | answers recv: %d msg/s\n", sent, answersRecv)
			atomic.SwapInt64(&sent, 0)
			atomic.SwapInt64(&answersRecv, 0)
		}
	}
}

// sendCmd represents the send command
var sendCmd = &cobra.Command{
	Use:   "send",
	Short: "starts UDP bench in send mode",
	Long:  `starts UDP bench in send mode`,
	Run: func(cmd *cobra.Command, args []string) {
		bufferPool = sync.Pool{New: func() interface{} { return make([]byte, 2048) }}
		done = make(chan os.Signal)
		log.Printf("starting sender with %d goroutines\n", numClients)
		go printStats()
		for i := 0; i < numClients; i++ {
			go send()
		}
		<-done
	},
}

func init() {
	sendCmd.Flags().IntVarP(&numClients, "clients", "c", 1, "the number of clients to use in the test")
	sendCmd.Flags().IntVarP(&rate, "rate", "r", 10, "the number of packets per second that each client will send")
	sendCmd.Flags().IntVarP(&packetSize, "packetSize", "s", 50, "the total size (in bytes) that the packets will have")
	sendCmd.Flags().StringVarP(&target, "target", "t", "localhost:5000", "the target address to send messages")
	sendCmd.Flags().BoolVarP(&shouldListenForAnswers, "shouldListenForAnswers", "a", false, "whether we should listen for answers in the sending conns")
	RootCmd.AddCommand(sendCmd)
}
