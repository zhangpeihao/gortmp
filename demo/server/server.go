package main

import (
	"flag"
	"fmt"
	"github.com/zhangpeihao/gortmp"
	"github.com/zhangpeihao/log"
	"os"
	"os/signal"
	"syscall"
)

const (
	programName = "RtmpServer"
	version     = "0.0.1"
)

var (
	address   *string = flag.String("Address", ":1935", "The address to bind.")
	mediaPath *string = flag.String("MediaPath", "./medias", "The media files folder.")
)

var (
	status uint
)

type ServerHandler struct{}

// InboundConn handler funcions
func (handler *ServerHandler) OnStatus(ibConn rtmp.InboundConn) {
	var err error
	status, err = ibConn.Status()
	fmt.Printf("@@@@@@@@@@@@@status: %d, err: %v\n", status, err)
}

func (handler *ServerHandler) OnStreamCreated(ibConn rtmp.InboundConn, stream rtmp.InboundStream) {
	fmt.Printf("Stream created: %d\n", stream.ID())
}

// Conn handler functions
func (handler *ServerHandler) Closed() {
	fmt.Printf("@@@@@@@@@@@@@Closed\n")
}

func (handler *ServerHandler) Received(message *rtmp.Message) {
}

func (handler *ServerHandler) ReceivedCommand(command *rtmp.Command) {
	fmt.Printf("ReceviedCommand: %+v\n", command)
}

// Stream handle functions
func (handler *ServerHandler) OnPlayStart(stream rtmp.InboundStream) {
	fmt.Printf("OnPlayStart\n")
}
func (handler *ServerHandler) OnPublishStart(stream rtmp.InboundStream) {
	fmt.Printf("OnPublishStart\n")
}
func (handler *ServerHandler) OnReceiveAudio(stream rtmp.InboundStream, on bool) {
	fmt.Printf("OnReceiveAudio: %b\n", on)
}
func (handler *ServerHandler) OnReceiveVideo(stream rtmp.InboundStream, on bool) {
	fmt.Printf("OnReceiveVideo: %b\n", on)
}

// Server handler functions
func (handler *ServerHandler) NewConnection(ibConn rtmp.InboundConn, connectReq *rtmp.Command,
	server *rtmp.Server) bool {
	fmt.Printf("NewConnection\n")
	ibConn.Attach(handler)
	return true
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	l := log.NewLogger(".", "server", nil, 60, 3600*24, true)
	rtmp.InitLogger(l)
	defer l.Close()
	handler := &ServerHandler{}
	server, err := rtmp.NewServer("tcp", *address, handler)
	if err != nil {
		fmt.Println("NewServer error", err)
		os.Exit(-1)
	}
	defer server.Close()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT)
	sig := <-ch
	fmt.Printf("Signal received: %v\n", sig)
}
