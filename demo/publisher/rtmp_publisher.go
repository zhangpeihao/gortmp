package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zhangpeihao/goflv"
	rtmp "github.com/zhangpeihao/gortmp"
	"github.com/zhangpeihao/log"
)

const (
	programName = "RtmpPublisher"
	version     = "0.0.1"
)

var (
	url         *string = flag.String("URL", "rtmp://192.168.20.111/vid3", "The rtmp url to connect.")
	streamName  *string = flag.String("Stream", "camstream", "Stream name to play.")
	flvFileName *string = flag.String("FLV", "", "FLV file to publishs.")
)

type TestOutboundConnHandler struct {
}

var obConn rtmp.OutboundConn
var createStreamChan chan rtmp.OutboundStream
var videoDataSize int64
var audioDataSize int64
var flvFile *flv.File

var status uint

func (handler *TestOutboundConnHandler) OnStatus(conn rtmp.OutboundConn) {
	var err error
	status, err = obConn.Status()
	fmt.Printf("@@@@@@@@@@@@@status: %d, err: %v\n", status, err)
}

func (handler *TestOutboundConnHandler) OnClosed(conn rtmp.Conn) {
	fmt.Printf("@@@@@@@@@@@@@Closed\n")
}

func (handler *TestOutboundConnHandler) OnReceived(conn rtmp.Conn, message *rtmp.Message) {
}

func (handler *TestOutboundConnHandler) OnReceivedRtmpCommand(conn rtmp.Conn, command *rtmp.Command) {
	fmt.Printf("ReceviedRtmpCommand: %+v\n", command)
}

func (handler *TestOutboundConnHandler) OnStreamCreated(conn rtmp.OutboundConn, stream rtmp.OutboundStream) {
	fmt.Printf("Stream created: %d\n", stream.ID())
	createStreamChan <- stream
}
func (handler *TestOutboundConnHandler) OnPlayStart(stream rtmp.OutboundStream) {

}
func (handler *TestOutboundConnHandler) OnPublishStart(stream rtmp.OutboundStream) {
	// Set chunk buffer size
	go publish(stream)
}

func publish(stream rtmp.OutboundStream) {

	var err error
	flvFile, err = flv.OpenFile(*flvFileName)
	if err != nil {
		fmt.Println("Open FLV dump file error:", err)
		return
	}
	defer flvFile.Close()
	startTs := uint32(0)
	startAt := time.Now().UnixNano()
	preTs := uint32(0)
	for status == rtmp.OUTBOUND_CONN_STATUS_CREATE_STREAM_OK {
		if flvFile.IsFinished() {
			fmt.Println("@@@@@@@@@@@@@@File finished")
			flvFile.LoopBack()
			startAt = time.Now().UnixNano()
			startTs = uint32(0)
			preTs = uint32(0)
		}
		header, data, err := flvFile.ReadTag()
		if err != nil {
			fmt.Println("flvFile.ReadTag() error:", err)
			break
		}
		switch header.TagType {
		case flv.VIDEO_TAG:
			videoDataSize += int64(len(data))
		case flv.AUDIO_TAG:
			audioDataSize += int64(len(data))
		}

		if startTs == uint32(0) {
			startTs = header.Timestamp
		}
		diff1 := uint32(0)
		//		deltaTs := uint32(0)
		if header.Timestamp > startTs {
			diff1 = header.Timestamp - startTs
		} else {
			fmt.Println("@@@@@@@@@@@@@@diff1")
		}
		if diff1 > preTs {
			//			deltaTs = diff1 - preTs
			preTs = diff1
		}
		if err = stream.PublishData(header.TagType, data, diff1); err != nil {
			fmt.Println("PublishData() error:", err)
			break
		}
		diff2 := uint32((time.Now().UnixNano() - startAt) / 1000000)
		//		fmt.Printf("diff1: %d, diff2: %d\n", diff1, diff2)
		if diff1 > diff2+100 {
			//			fmt.Printf("header.Timestamp: %d, now: %d\n", header.Timestamp, time.Now().UnixNano())
			time.Sleep(time.Millisecond * time.Duration(diff1-diff2))
		}
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	l := log.NewLogger(".", "publisher", nil, 60, 3600*24, true)
	rtmp.InitLogger(l)
	defer l.Close()
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
	fmt.Println("to connect")
	err = obConn.Connect()
	if err != nil {
		fmt.Printf("Connect error: %s", err.Error())
		os.Exit(-1)
	}
	for {
		select {
		case stream := <-createStreamChan:
			// Publish
			stream.Attach(testHandler)
			err = stream.Publish(*streamName, "live")
			if err != nil {
				fmt.Printf("Publish error: %s", err.Error())
				os.Exit(-1)
			}

		case <-time.After(1 * time.Second):
			fmt.Printf("Audio size: %d bytes; Vedio size: %d bytes\n", audioDataSize, videoDataSize)
		}
	}
}
