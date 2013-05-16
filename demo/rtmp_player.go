package main

import (
	"flag"
	"fmt"
	"github.com/zhangpeihao/goflv"
	"github.com/zhangpeihao/gortmp"
	"os"
	"time"
)

const (
	programName = "RtmpPlayer"
	version     = "0.0.1"
)

var (
	url        *string = flag.String("URL", "rtmp://192.168.20.111/vid3", "The rtmp url to connect.")
	streamName *string = flag.String("Stream", "camstream", "Stream name to play.")
	dumpFlv    *string = flag.String("DumpFLV", "", "Dump FLV into file.")
)

type TestOutboundConnHandler struct {
}

var obConn rtmp.OutboundConn
var createStreamChan chan rtmp.OutboundStream
var videoDataSize int64
var audioDataSize int64
var flvFile *flv.File

func (handler *TestOutboundConnHandler) OnStatus() {
	status, err := obConn.Status()
	fmt.Printf("status: %d, err: %v\n", status, err)
}

func (handler *TestOutboundConnHandler) Received(message *rtmp.Message) {
	switch message.Type {
	case rtmp.VIDEO_TYPE:
		if flvFile != nil {
			flvFile.WriteVideoTag(message.Buf.Bytes(), message.Timestamp)
		}
		videoDataSize += int64(message.Buf.Len())
	case rtmp.AUDIO_TYPE:
		if flvFile != nil {
			flvFile.WriteAudioTag(message.Buf.Bytes(), message.Timestamp)
		}
		audioDataSize += int64(message.Buf.Len())
	}
}

func (handler *TestOutboundConnHandler) ReceivedCommand(command *rtmp.Command) {
	fmt.Printf("ReceviedCommand: %+v\n", command)
}

func (handler *TestOutboundConnHandler) OnStreamCreated(stream rtmp.OutboundStream) {
	fmt.Printf("Stream created: %d\n", stream.ID())
	createStreamChan <- stream
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	// Create flv file
	if len(*dumpFlv) > 0 {
		var err error
		flvFile, err = flv.CreateFile(*dumpFlv)
		if err != nil {
			fmt.Println("Create FLV dump file error:", err)
			return
		}
	}
	defer func() {
		if flvFile != nil {
			flvFile.Close()
		}
	}()

	createStreamChan = make(chan rtmp.OutboundStream)
	testHandler := &TestOutboundConnHandler{}
	fmt.Println("to dial")
	var err error
	obConn, err = rtmp.Dial(*url, testHandler, 100)
	if err != nil {
		fmt.Println("Dial error", err)
		os.Exit(-1)
	}
	defer obConn.Close()
	fmt.Printf("obConn: %+v\n", obConn)
	fmt.Printf("obConn.URL(): %s\n", obConn.URL())
	fmt.Println("to connect")
	//	err = obConn.Connect("33abf6e996f80e888b33ef0ea3a32bfd", "131228035", "161114738", "play", "", "", "1368083579")
	err = obConn.Connect()
	if err != nil {
		fmt.Printf("Connect error: %s", err.Error())
		os.Exit(-1)
	}
	for {
		select {
		case stream := <-createStreamChan:
			// Play
			err = stream.Play(*streamName, nil, nil, nil)
			if err != nil {
				fmt.Printf("Play error: %s", err.Error())
				os.Exit(-1)
			}
			// Set Buffer Length

		case <-time.After(1 * time.Second):
			fmt.Printf("Audio size: %d bytes; Vedio size: %d bytes\n", audioDataSize, videoDataSize)
		}
	}
}
