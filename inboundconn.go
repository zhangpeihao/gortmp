// Copyright 2013, zhangpeihao All rights reserved.

package rtmp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
	"net"
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
	// Connect parameters
	Params() map[string]string
	// Attach handler
	Attach(handler InboundConnHandler)
}

type inboundConn struct {
	app         string
	params      map[string]string
	handler     InboundConnHandler
	authHandler InboundAuthHandler
	conn        Conn
	status      uint
	err         error
	streams     map[uint32]InboundStream
}

func NewInboundConn(c net.Conn, br *bufio.Reader, bw *bufio.Writer,
	authHandler InboundAuthHandler, maxChannelNumber int) (InboundConn, error) {
	ibConn := &inboundConn{
		authHandler: authHandler,
		status:      INBOUND_CONN_STATUS_CLOSE,
		params:      make(map[string]string),
		streams:     make(map[uint32]InboundStream),
	}
	ibConn.conn = NewConn(c, br, bw, ibConn, maxChannelNumber)
	return ibConn, nil
}

// Callback when recieved message. Audio & Video data
func (ibConn *inboundConn) Received(message *Message) {
	stream, found := ibConn.streams[message.StreamID]
	if found {
		if !stream.Received(message) {
			ibConn.handler.Received(message)
		}
	} else {
		ibConn.handler.Received(message)
	}
}

// Callback when recieved message.
func (ibConn *inboundConn) ReceivedCommand(command *Command) {
	command.Dump()
	switch command.Name {
	case "connect":
		ibConn.onConnect(command)
		// Connect from client
	case "createStream":
		// Create a new stream
		ibConn.onCreateStream(command)
	default:
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE, "inboundConn::ReceivedCommand: %+v\n", command)
	}
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

// Connection closed
func (ibConn *inboundConn) Closed() {
	ibConn.status = INBOUND_CONN_STATUS_CLOSE
	ibConn.handler.OnStatus(ibConn)
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

func (ibConn *inboundConn) Params() map[string]string {
	return ibConn.params
}

// Connection status
func (ibConn *inboundConn) Status() (uint, error) {
	return ibConn.status, ibConn.err
}
func (ibConn *inboundConn) Attach(handler InboundConnHandler) {
	ibConn.handler = handler
}
func (ibConn *inboundConn) onConnect(cmd *Command) {
	if cmd.Objects == nil {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect cmd.Object == nil\n")
		ibConn.SendErrorResult(cmd)
		return
	}
	if len(cmd.Objects) == 0 {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect len(cmd.Object) == 0\n")
		ibConn.SendErrorResult(cmd)
		return
	}
	params, ok := cmd.Objects[0].(amf.Object)
	if !ok {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect cmd.Object[0] is not an amd object\n")
		ibConn.SendErrorResult(cmd)
		return
	}

	// Get app
	app, found := params["app"]
	if !found {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect no app value in cmd.Object[0]\n")
		ibConn.SendErrorResult(cmd)
		return
	}
	ibConn.app, ok = app.(string)
	if !ok {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundConn::onConnect cmd.Object[0].app is not a string\n")
		ibConn.SendErrorResult(cmd)
		return
	}

	// Todo: Get version for log
	// Todo: Get other paramters
	// Todo: Auth by logical
	if ibConn.authHandler.OnConnectAuth(ibConn, cmd) {
		ibConn.SendSucceededResult(cmd)
	} else {
		ibConn.SendErrorResult(cmd)
	}
}

func (ibConn *inboundConn) onCreateStream(cmd *Command) {
}

func (ibConn *inboundConn) SendSucceededResult(req *Command) {
	obj1 := make(amf.Object)
	obj1["fmsVer"] = fmt.Sprintf("FMS/%s", FMS_VERSION_STRING)
	obj1["capabilities"] = float64(255)
	obj2 := make(amf.Object)
	obj2["level"] = "status"
	obj2["code"] = RESULT_CONNECT_OK
	obj2["description"] = RESULT_CONNECT_OK_DESC
	ibConn.SendResult(req, "_result", obj1, obj2)
}

func (ibConn *inboundConn) SendErrorResult(req *Command) {
	obj2 := make(amf.Object)
	obj2["level"] = "status"
	obj2["code"] = RESULT_CONNECT_REJECTED
	obj2["description"] = RESULT_CONNECT_REJECTED_DESC
	ibConn.SendResult(req, "_error", nil, obj2)
}

func (ibConn *inboundConn) SendResult(req *Command, name string, obj1, obj2 interface{}) (err error) {
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
	CheckError(err, "inboundConn::SendResult() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("SendResult")
	return ibConn.conn.Send(message)

}
