// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/Shopify/sarama"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

type DataMon struct {
	Code        byte
	Pseq        uint16
	Plen        uint8
	ServerStart uint32
	// files []struct {
	// 	recType byte
	// 	recFlag byte
	// 	recSize [2]byte
	// 	fileID  [4]byte
	// 	read    int8
	// 	readv   int8
	// 	write   int8
	// }
}

type MsgMon struct {
	RecType uint8
	RecFlag uint8
	RecSize uint16
	FileID  uint32
	Read    int8
	Readv   int8
	Write   int8
}

var (
	serverPort = flag.String("serverPort", os.Getenv("COLLECTOR_PORT"), "The metrics collector port")
	path       = flag.String("path", os.Getenv("CACHE_META_PATH"), "The path where cache metadata are stored")
	brokers    = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	topic      = flag.String("topic", "default_topic", "The Kafka topic to use")
	certFile   = flag.String("certificate", "", "The optional certificate file for client authentication")
	keyFile    = flag.String("key", "", "The optional key file for client authentication")
	caFile     = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySSL  = flag.Bool("verify", false, "Optional verify ssl certificates chain")
	useTLS     = flag.Bool("tls", false, "Use TLS to communicate with the cluster")

	logger = log.New(os.Stdout, "[Producer] ", log.LstdFlags)
)

func createTLSConfiguration() (t *tls.Config) {
	t = &tls.Config{
		InsecureSkipVerify: *verifySSL,
	}
	if *certFile != "" && *keyFile != "" && *caFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *verifySSL,
		}
	}
	return t
}

// CheckError ...
func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}

func main() {
	// TODO: UPD listener with translator and send to kafka

	flag.Parse()

	conf := sarama.NewConfig()
	conf.Producer.Retry.Max = 1
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Metadata.Full = true
	conf.Version = sarama.V0_10_0_0
	conf.ClientID = "test_client"
	conf.Metadata.Full = true

	// instantiate server listner
	ServerAddr, err := net.ResolveUDPAddr("udp", ":"+*serverPort)
	CheckError(err)

	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	CheckError(err)
	defer ServerConn.Close()

	buf := make([]byte, 1024)

	fmt.Println("Server started on port", *serverPort)

	for {
		n, _, err := ServerConn.ReadFromUDP(buf)
		//fmt.Println("Received ", string(buf[0:n]), " from ", addr)
		if err != nil {
			fmt.Println("Error: ", err)
		}

		translate(buf[0:n])

	}
	// send to kafka
	/*

		if *brokers == "" {
			log.Fatalln("at least one brocker is required")
		}

		if *useTLS {
			conf.Net.TLS.Enable = true
			conf.Net.TLS.Config = createTLSConfiguration()
		}

		syncProcuder, err := sarama.NewSyncProducer(strings.Split(*brokers, ","), conf)
		if err != nil {
			logger.Fatalln("failed to create producer: ", err)
		}

		err = folderScan(*path, syncProcuder)
		if err != nil {
			logger.Fatalln("failed to send files: ", err)
		}

		_ = syncProcuder.Close()
	*/
}

func translate(b []byte) string {
	//b := []byte{0x18, 0x2d, 0x44, 0x54, 0xfb, 0x21, 0x09, 0x40, 0xff, 0x01, 0x02, 0x03, 0xbe, 0xef}
	r := bytes.NewReader(b[:8])

	var data DataMon
	//fmt.Println("Received head: ", string(b[0]))
	//fmt.Println("Received msg: ", string(b[1:]))
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		fmt.Println("binary.Read failed:", err)
	}

	// TODO: write in file and read with python

	fmt.Println("code", string(data.Code))
	fmt.Println("b[:8]", b[:8])
	fmt.Println("plen", data.Plen)
	fmt.Println("pseq", data.Pseq)
	fmt.Println("start", data.ServerStart)

	fmt.Println("len b:", len(b))
	fmt.Println("----")

	if len(b) != int(data.Plen) {
		fmt.Println("error: excpected size", int(data.Plen), "but receiced", len(b))
		return ""
	}

	if len(b) > 8 && string(data.Code) == "f" {

		fmt.Println("RecType", uint8(b[8]))

		var h struct {
			RecType uint8
			RecFlag uint16
			RecSize uint8
			FileID  uint32
		}

		b = b[8:]

		r = bytes.NewReader(b)
		if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
			fmt.Println("binary.Read failed:", err)
		}

		if h.RecType == 2 {
			//fmt.Println("b[8:32]", b[8:32])
			fmt.Println("FileID", h.FileID)
			fmt.Println("RecSize", h.RecSize)
			fmt.Println("----")

			b = b[h.RecSize:]

			if h.RecSize > 16 {

				var filetime struct {
					Rectype   uint8
					RecFlag   uint16
					RecSize   uint8
					IsXfrrecs uint16
					Totalrecs uint16
					Sid       uint32
					Teserved  uint32
					TBeg      uint32
					TEnd      uint32
				}

				r = bytes.NewReader(b)
				if err := binary.Read(r, binary.LittleEndian, &filetime); err != nil {
					fmt.Println("binary.Read failed:", err)
				}

				//fmt.Println("b", b)
				fmt.Println("totalrecs:", filetime.Totalrecs)

			} else {

			}

			for {
				r = bytes.NewReader(b)
				if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
					fmt.Println("binary.Read failed:", err)
				}

				fmt.Println("Rec2 type:", h.RecType)
				fmt.Println("Rec2 flag:", h.RecFlag)
				fmt.Println("Rec2 len:", h.RecSize)
				fmt.Println("Rec2 len b:", len(b))
				fmt.Println("++++++")

				// 	if up[0]==0: # isClose
				// 	if up[1] & 0b010:  #hasOPS
				// 		O=ops._make(struct.unpack("!IIIHHQIIIIII",d[32:80]))
				// 		#print O
				// 	else:
				// 		O=()
				// 	#forced Disconnect prior to close  forced =0x01, hasOPS =0x02, hasSSQ =0x04
				// 	return fileClose._make(struct.unpack("!BBHIQQQ",d[:32]))
				// elif up[0]==1: # isOpen
				// 	fO=struct.unpack("!BBHIQ",d[:16])
				// 	if up[1]==1:
				// 		userId=struct.unpack("!I",d[16:20])[0]
				// 		fileName=struct.unpack("!"+str(up[2]-20)+"s",d[20:up[2]])[0].rstrip('\0')
				// 	else:
				// 		userId=0
				// 		fileName=''
				// 	return fileOpen._make(fO + (userId,fileName))

				if int(h.RecSize) < len(b) {
					b = b[h.RecSize:]
				} else {
					break
				}

			}

		} else if h.RecType == 0 {
			r = bytes.NewReader(b[:32])

			var close struct {
				Rectype uint8
				RecFlag uint8
				RecSize uint16
				FileID  uint32
				read    uint64
				readv   uint64
				write   uint64
			}

			if err := binary.Read(r, binary.LittleEndian, &close); err != nil {
				fmt.Println("binary.Read failed:", err)
			}

			fmt.Println("RecClose type:", close.FileID)
			fmt.Println("RecClose flag:", close.read)
			fmt.Println("RecClose len:", close.readv)
			fmt.Println("++++++")

		}
	}

	return string(b)
	/*
		var data struct {
			PI   float64
			Uate uint8
			Mine [3]byte
			Too  uint16
		}

		if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
			fmt.Println("binary.Read failed:", err)
		}

		fmt.Println(data.PI)
		fmt.Println(data.Uate)
		fmt.Printf("% x\n", data.Mine)
		fmt.Println(data.Too)
		//x1 := binary.LittleEndian.Uint16(b[0:])
		//x2 := binary.LittleEndian.Uint16(b[2:])
		//fmt.Printf("%#04x %#04x\n", x1, x2)
		return string(data.Mine[:])
	*/
}
