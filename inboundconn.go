// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
	"net"
	"sync"
	"time"
)

const (
	INBOUND_CONN_STATUS_CLOSE            = uint(0)
	INBOUND_CONN_STATUS_CONNECT_OK       = uint(1)
	INBOUND_CONN_STATUS_CREATE_STREAM_OK = uint(2)
)

// A handler for inbound connection
type InboundAuthHandler interface {
	OnConnectAuth(ibConn InboundConn, connectReq *Command) bool
}

// A handler for inbound connection
type InboundConnHandler interface {
	ConnHandler
	// When connection status changed
	OnStatus(ibConn InboundConn)
	// On stream created
	OnStreamCreated(ibConn InboundConn, stream InboundStream)
	// On stream closed
	OnStreamClosed(ibConn InboundConn, stream InboundStream)
}

type InboundConn interface {
	// Close a connection
	Close()
	// Connection status
	Status() (uint, error)
	// Send a message
	Send(message *Message) error
	// Calls a command or method on Flash Media Server
	// or on an application server running Flash Remoting.
	Call(customParameters ...interface{}) (err error)
	// Get network connect instance
	Conn() Conn
	// Attach handler
	Attach(handler InboundConnHandler)
	// Get connect request
	ConnectRequest() *Command
}

type inboundConn struct {
	connectReq    *Command
	app           string
	handler       InboundConnHandler
	authHandler   InboundAuthHandler
	conn          Conn
	status        uint
	err           error
	streams       map[uint32]*inboundStream
	streamsLocker sync.Mutex
}

func NewInboundConn(c net.Conn, br *bufio.Reader, bw *bufio.Writer,
	authHandler InboundAuthHandler, maxChannelNumber int) (InboundConn, error) {
	ibConn := &inboundConn{
		authHandler: authHandler,
		status:      INBOUND_CONN_STATUS_CLOSE,
		streams:     make(map[uint32]*inboundStream),
	}
	ibConn.conn = NewConn(c, br, bw, ibConn, maxChannelNumber)
	return ibConn, nil
}

// Callback when recieved message. Audio & Video data
func (ibConn *inboundConn) OnReceived(conn Conn, message *Message) {
	stream, found := ibConn.streams[message.StreamID]
	if found {
		if !stream.Received(message) {
			ibConn.handler.OnReceived(ibConn.conn, message)
		}
	} else {
		ibConn.handler.OnReceived(ibConn.conn, message)
	}
}

// Callback when recieved message.
func (ibConn *inboundConn) OnReceivedRtmpCommand(conn Conn, command *Command) {
	command.Dump()
	switch command.Name {
	case "connect":
		ibConn.onConnect(command)
		// Connect from client
	case "createStream":
		// Create a new stream
		ibConn.onCreateStream(command)
	default:
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE, "inboundConn::ReceivedRtmpCommand: %+v\n", command)
	}
}

// Connection closed
func (ibConn *inboundConn) OnClosed(conn Conn) {
	ibConn.status = INBOUND_CONN_STATUS_CLOSE
	ibConn.handler.OnStatus(ibConn)
}

// Close a connection
func (ibConn *inboundConn) Close() {
	for _, stream := range ibConn.streams {
		stream.Close()
	}
	time.Sleep(time.Second)
	ibConn.status = INBOUND_CONN_STATUS_CLOSE
	ibConn.conn.Close()
}

// Send a message
func (ibConn *inboundConn) Send(message *Message) error {
	return ibConn.conn.Send(message)
}

// Calls a command or method on Flash Media Server
// or on an application server running Flash Remoting.
func (ibConn *inboundConn) Call(customParameters ...interface{}) (err error) {
	return errors.New("Unimplemented")
}

// Get network connect instance
func (ibConn *inboundConn) Conn() Conn {
	return ibConn.conn
}

// Connection status
func (ibConn *inboundConn) Status() (uint, error) {
	return ibConn.status, ibConn.err
}
func (ibConn *inboundConn) Attach(handler InboundConnHandler) {
	ibConn.handler = handler
}

////////////////////////////////
// Local functions

func (ibConn *inboundConn) allocStream(stream *inboundStream) uint32 {
	ibConn.streamsLocker.Lock()
	i := uint32(1)
	for {
		_, found := ibConn.streams[i]
		if !found {
			ibConn.streams[i] = stream
			stream.id = i
			break
		}
		i++
	}
	ibConn.streamsLocker.Unlock()
	return i
}

func (ibConn *inboundConn) releaseStream(streamID uint32) {
	ibConn.streamsLocker.Lock()
	delete(ibConn.streams, streamID)
	ibConn.streamsLocker.Unlock()
}

func (ibConn *inboundConn) onConnect(cmd *Command) {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"inboundConn::onConnect")
	ibConn.connectReq = cmd
	if cmd.Objects == nil {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect cmd.Object == nil\n")
		ibConn.sendConnectErrorResult(cmd)
		return
	}
	if len(cmd.Objects) == 0 {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect len(cmd.Object) == 0\n")
		ibConn.sendConnectErrorResult(cmd)
		return
	}
	params, ok := cmd.Objects[0].(amf.Object)
	if !ok {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect cmd.Object[0] is not an amd object\n")
		ibConn.sendConnectErrorResult(cmd)
		return
	}

	// Get app
	app, found := params["app"]
	if !found {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect no app value in cmd.Object[0]\n")
		ibConn.sendConnectErrorResult(cmd)
		return
	}
	ibConn.app, ok = app.(string)
	if !ok {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect cmd.Object[0].app is not a string\n")
		ibConn.sendConnectErrorResult(cmd)
		return
	}

	// Todo: Get version for log
	// Todo: Get other paramters
	// Todo: Auth by logical
	if ibConn.authHandler.OnConnectAuth(ibConn, cmd) {
		ibConn.conn.SetWindowAcknowledgementSize()
		ibConn.conn.SetPeerBandwidth(2500000, SET_PEER_BANDWIDTH_DYNAMIC)
		ibConn.conn.SetChunkSize(4096)
		ibConn.sendConnectSucceededResult(cmd)
	} else {
		ibConn.sendConnectErrorResult(cmd)
	}
}

func (ibConn *inboundConn) onCreateStream(cmd *Command) {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"inboundConn::onCreateStream")
	// New inbound stream
	newChunkStream, err := ibConn.conn.CreateMediaChunkStream()
	if err != nil {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"outboundConn::ReceivedCommand() CreateMediaChunkStream err:", err)
		return
	}
	stream := &inboundStream{
		conn:          ibConn,
		chunkStreamID: newChunkStream.ID,
	}
	ibConn.allocStream(stream)
	ibConn.status = INBOUND_CONN_STATUS_CREATE_STREAM_OK
	ibConn.handler.OnStatus(ibConn)
	ibConn.handler.OnStreamCreated(ibConn, stream)
	// Response result
	ibConn.sendCreateStreamSuccessResult(cmd)
}

func (ibConn *inboundConn) onCloseStream(stream *inboundStream) {
	ibConn.releaseStream(stream.id)
	ibConn.handler.OnStreamClosed(ibConn, stream)
}

func (ibConn *inboundConn) sendConnectSucceededResult(req *Command) {
	obj1 := make(amf.Object)
	obj1["fmsVer"] = fmt.Sprintf("FMS/%s", FMS_VERSION_STRING)
	obj1["capabilities"] = float64(255)
	obj2 := make(amf.Object)
	obj2["level"] = "status"
	obj2["code"] = RESULT_CONNECT_OK
	obj2["description"] = RESULT_CONNECT_OK_DESC
	ibConn.sendConnectResult(req, "_result", obj1, obj2)
}

func (ibConn *inboundConn) sendConnectErrorResult(req *Command) {
	obj2 := make(amf.Object)
	obj2["level"] = "status"
	obj2["code"] = RESULT_CONNECT_REJECTED
	obj2["description"] = RESULT_CONNECT_REJECTED_DESC
	ibConn.sendConnectResult(req, "_error", nil, obj2)
}

func (ibConn *inboundConn) sendConnectResult(req *Command, name string, obj1, obj2 interface{}) (err error) {
	// Create createStream command
	cmd := &Command{
		IsFlex:        false,
		Name:          name,
		TransactionID: req.TransactionID,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = obj1
	cmd.Objects[1] = obj2
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "inboundConn::sendConnectResult() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("sendConnectResult")
	return ibConn.conn.Send(message)

}

func (ibConn *inboundConn) sendCreateStreamSuccessResult(req *Command) (err error) {
	// Create createStream command
	cmd := &Command{
		IsFlex:        false,
		Name:          "_result",
		TransactionID: req.TransactionID,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = int32(1)
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "inboundConn::sendCreateStreamSuccessResult() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("sendCreateStreamSuccessResult")
	return ibConn.conn.Send(message)

}

func (ibConn *inboundConn) ConnectRequest() *Command {
	return ibConn.connectReq
}
