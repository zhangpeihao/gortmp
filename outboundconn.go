// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
	"net"
	"time"
)

const (
	OUTBOUND_CONN_STATUS_CLOSE            = uint(0)
	OUTBOUND_CONN_STATUS_HANDSHAKE_OK     = uint(1)
	OUTBOUND_CONN_STATUS_CONNECT          = uint(2)
	OUTBOUND_CONN_STATUS_CONNECT_OK       = uint(3)
	OUTBOUND_CONN_STATUS_CREATE_STREAM    = uint(4)
	OUTBOUND_CONN_STATUS_CREATE_STREAM_OK = uint(5)
)

// A handler for outbound connection
type OutboundConnHandler interface {
	ConnHandler
	// When connection status changed
	OnStatus(obConn OutboundConn)
	// On stream created
	OnStreamCreated(obConn OutboundConn, stream OutboundStream)
}

type OutboundConn interface {
	// Connect an appliction on FMS after handshake.
	Connect(extendedParameters ...interface{}) (err error)
	// Create a stream
	CreateStream() (err error)
	// Close a connection
	Close()
	// URL to connect
	URL() string
	// Connection status
	Status() (uint, error)
	// Send a message
	Send(message *Message) error
	// Calls a command or method on Flash Media Server
	// or on an application server running Flash Remoting.
	Call(name string, customParameters ...interface{}) (err error)
	// Get network connect instance
	Conn() Conn
}

// High-level interface
//
// A RTMP connection(based on TCP) to RTMP server(FMS or crtmpserver).
// In one connection, we can create many chunk streams.
type outboundConn struct {
	url          string
	rtmpURL      RtmpURL
	status       uint
	err          error
	handler      OutboundConnHandler
	conn         Conn
	transactions map[uint32]string
	streams      map[uint32]OutboundStream
}

// Connect to FMS server, and finish handshake process
func Dial(url string, handler OutboundConnHandler, maxChannelNumber int) (OutboundConn, error) {
	rtmpURL, err := ParseURL(url)
	if err != nil {
		return nil, err
	}
	var c net.Conn
	switch rtmpURL.protocol {
	case "rtmp":
		c, err = net.Dial("tcp", fmt.Sprintf("%s:%d", rtmpURL.host, rtmpURL.port))
	case "rtmps":
		c, err = tls.Dial("tcp", fmt.Sprintf("%s:%d", rtmpURL.host, rtmpURL.port), &tls.Config{InsecureSkipVerify: true})
	default:
		err = errors.New(fmt.Sprintf("Unsupport protocol %s", rtmpURL.protocol))
	}
	if err != nil {
		return nil, err
	}

	ipConn, ok := c.(*net.TCPConn)
	if ok {
		ipConn.SetWriteBuffer(128 * 1024)
	}
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	timeout := time.Duration(10*time.Second)
	err = Handshake(c, br, bw, timeout)
	//err = HandshakeSample(c, br, bw, timeout)
	if err == nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG, "Handshake OK")

		obConn := &outboundConn{
			url:          url,
			rtmpURL:      rtmpURL,
			handler:      handler,
			status:       OUTBOUND_CONN_STATUS_HANDSHAKE_OK,
			transactions: make(map[uint32]string),
			streams:      make(map[uint32]OutboundStream),
		}
		obConn.handler.OnStatus(obConn)
		obConn.conn = NewConn(c, br, bw, obConn, maxChannelNumber)
		return obConn, nil
	}

	return nil, err
}

// Connect to FMS server, and finish handshake process
func NewOutbounConn(c net.Conn, url string, handler OutboundConnHandler, maxChannelNumber int) (OutboundConn, error) {
	rtmpURL, err := ParseURL(url)
	if err != nil {
		return nil, err
	}
	if rtmpURL.protocol != "rtmp" {
		return nil, errors.New(fmt.Sprintf("Unsupport protocol %s", rtmpURL.protocol))
	}
	/*
		ipConn, ok := c.(*net.TCPConn)
		if ok {
			ipConn.SetWriteBuffer(128 * 1024)
		}
	*/
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	obConn := &outboundConn{
		url:          url,
		rtmpURL:      rtmpURL,
		handler:      handler,
		status:       OUTBOUND_CONN_STATUS_HANDSHAKE_OK,
		transactions: make(map[uint32]string),
		streams:      make(map[uint32]OutboundStream),
	}
	obConn.conn = NewConn(c, br, bw, obConn, maxChannelNumber)
	return obConn, nil
}

// Connect an appliction on FMS after handshake.
func (obConn *outboundConn) Connect(extendedParameters ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if obConn.err == nil {
				obConn.err = err
			}
		}
	}()
	// Create connect command
	buf := new(bytes.Buffer)
	// Command name
	_, err = amf.WriteString(buf, "connect")
	CheckError(err, "Connect() Write name: connect")
	transactionID := obConn.conn.NewTransactionID()
	obConn.transactions[transactionID] = "connect"
	_, err = amf.WriteDouble(buf, float64(transactionID))
	CheckError(err, "Connect() Write transaction ID")
	_, err = amf.WriteObjectMarker(buf)
	CheckError(err, "Connect() Write object marker")

	_, err = amf.WriteObjectName(buf, "app")
	CheckError(err, "Connect() Write app name")
	_, err = amf.WriteString(buf, obConn.rtmpURL.App())
	CheckError(err, "Connect() Write app value")

	_, err = amf.WriteObjectName(buf, "flashVer")
	CheckError(err, "Connect() Write flashver name")
	_, err = amf.WriteString(buf, FLASH_PLAYER_VERSION_STRING)
	CheckError(err, "Connect() Write flashver value")

	//	_, err = amf.WriteObjectName(buf, "swfUrl")
	//	CheckError(err, "Connect() Write swfUrl name")
	//	_, err = amf.WriteString(buf, SWF_URL_STRING)
	//	CheckError(err, "Connect() Write swfUrl value")

	_, err = amf.WriteObjectName(buf, "tcUrl")
	CheckError(err, "Connect() Write tcUrl name")
	_, err = amf.WriteString(buf, obConn.url)
	CheckError(err, "Connect() Write tcUrl value")

	_, err = amf.WriteObjectName(buf, "fpad")
	CheckError(err, "Connect() Write fpad name")
	_, err = amf.WriteBoolean(buf, false)
	CheckError(err, "Connect() Write fpad value")

	_, err = amf.WriteObjectName(buf, "capabilities")
	CheckError(err, "Connect() Write capabilities name")
	_, err = amf.WriteDouble(buf, DEFAULT_CAPABILITIES)
	CheckError(err, "Connect() Write capabilities value")

	_, err = amf.WriteObjectName(buf, "audioCodecs")
	CheckError(err, "Connect() Write audioCodecs name")
	_, err = amf.WriteDouble(buf, DEFAULT_AUDIO_CODECS)
	CheckError(err, "Connect() Write audioCodecs value")

	_, err = amf.WriteObjectName(buf, "videoCodecs")
	CheckError(err, "Connect() Write videoCodecs name")
	_, err = amf.WriteDouble(buf, DEFAULT_VIDEO_CODECS)
	CheckError(err, "Connect() Write videoCodecs value")

	_, err = amf.WriteObjectName(buf, "videoFunction")
	CheckError(err, "Connect() Write videoFunction name")
	_, err = amf.WriteDouble(buf, float64(1))
	CheckError(err, "Connect() Write videoFunction value")

	//	_, err = amf.WriteObjectName(buf, "pageUrl")
	//	CheckError(err, "Connect() Write pageUrl name")
	//	_, err = amf.WriteString(buf, PAGE_URL_STRING)
	//	CheckError(err, "Connect() Write pageUrl value")

	//_, err = amf.WriteObjectName(buf, "objectEncoding")
	//CheckError(err, "Connect() Write objectEncoding name")
	//_, err = amf.WriteDouble(buf, float64(amf.AMF0))
	//CheckError(err, "Connect() Write objectEncoding value")

	_, err = amf.WriteObjectEndMarker(buf)
	CheckError(err, "Connect() Write ObjectEndMarker")

	// extended parameters
	for _, param := range extendedParameters {
		_, err = amf.WriteValue(buf, param)
		CheckError(err, "Connect() Write extended parameters")
	}
	connectMessage := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	connectMessage.Dump("connect")
	obConn.status = OUTBOUND_CONN_STATUS_CONNECT
	return obConn.conn.Send(connectMessage)
}

// Close a connection
func (obConn *outboundConn) Close() {
	for _, stream := range obConn.streams {
		stream.Close()
	}
	obConn.status = OUTBOUND_CONN_STATUS_CLOSE
	go func() {
		time.Sleep(time.Second)
		obConn.conn.Close()
	}()
}

// URL to connect
func (obConn *outboundConn) URL() string {
	return obConn.url
}

// Connection status
func (obConn *outboundConn) Status() (uint, error) {
	return obConn.status, obConn.err
}

// Callback when recieved message. Audio & Video data
func (obConn *outboundConn) OnReceived(conn Conn, message *Message) {
	stream, found := obConn.streams[message.StreamID]
	if found {
		if !stream.Received(message) {
			obConn.handler.OnReceived(conn, message)
		}
	} else {
		obConn.handler.OnReceived(conn, message)
	}
}

// Callback when recieved message.
func (obConn *outboundConn) OnReceivedRtmpCommand(conn Conn, command *Command) {
	command.Dump()
	switch command.Name {
	case "_result":
		transaction, found := obConn.transactions[command.TransactionID]
		if found {
			switch transaction {
			case "connect":
				if command.Objects != nil && len(command.Objects) >= 2 {
					information, ok := command.Objects[1].(amf.Object)
					if ok {
						code, ok := information["code"]
						if ok && code == RESULT_CONNECT_OK {
							// Connect OK
							//time.Sleep(time.Duration(200) * time.Millisecond)
							obConn.conn.SetWindowAcknowledgementSize()
							obConn.status = OUTBOUND_CONN_STATUS_CONNECT_OK
							obConn.handler.OnStatus(obConn)
							obConn.status = OUTBOUND_CONN_STATUS_CREATE_STREAM
							obConn.CreateStream()
						}
					}
				}
			case "createStream":
				if command.Objects != nil && len(command.Objects) >= 2 {
					streamID, ok := command.Objects[1].(float64)
					if ok {
						newChunkStream, err := obConn.conn.CreateMediaChunkStream()
						if err != nil {
							logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
								"outboundConn::ReceivedRtmpCommand() CreateMediaChunkStream err:", err)
							return
						}
						stream := &outboundStream{
							id:            uint32(streamID),
							conn:          obConn,
							chunkStreamID: newChunkStream.ID,
						}
						obConn.streams[stream.ID()] = stream
						obConn.status = OUTBOUND_CONN_STATUS_CREATE_STREAM_OK
						obConn.handler.OnStatus(obConn)
						obConn.handler.OnStreamCreated(obConn, stream)
					}
				}
			}
			delete(obConn.transactions, command.TransactionID)
		}
	case "_error":
		transaction, found := obConn.transactions[command.TransactionID]
		if found {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
				"Command(%d) %s error\n", command.TransactionID, transaction)
		} else {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
				"Command(%d) not been found\n", command.TransactionID)
		}
	case "onBWCheck":
	}
	obConn.handler.OnReceivedRtmpCommand(obConn.conn, command)
}

// Connection closed
func (obConn *outboundConn) OnClosed(conn Conn) {
	obConn.status = OUTBOUND_CONN_STATUS_CLOSE
	obConn.handler.OnStatus(obConn)
	obConn.handler.OnClosed(conn)
}

// Create a stream
func (obConn *outboundConn) CreateStream() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if obConn.err == nil {
				obConn.err = err
			}
		}
	}()
	// Create createStream command
	transactionID := obConn.conn.NewTransactionID()
	cmd := &Command{
		IsFlex:        false,
		Name:          "createStream",
		TransactionID: transactionID,
		Objects:       make([]interface{}, 1),
	}
	cmd.Objects[0] = nil
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "createStream() Create command")
	obConn.transactions[transactionID] = "createStream"

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("createStream")
	return obConn.conn.Send(message)
}

// Send a message
func (obConn *outboundConn) Send(message *Message) error {
	return obConn.conn.Send(message)
}

// Calls a command or method on Flash Media Server
// or on an application server running Flash Remoting.
func (obConn *outboundConn) Call(name string, customParameters ...interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			if obConn.err == nil {
				obConn.err = err
			}
		}
	}()
	// Create command
	transactionID := obConn.conn.NewTransactionID()
	cmd := &Command{
		IsFlex:        false,
		Name:          name,
		TransactionID: transactionID,
		Objects:       make([]interface{}, 1+len(customParameters)),
	}
	cmd.Objects[0] = nil
	for index, param := range customParameters {
		cmd.Objects[index+1] = param
	}
	buf := new(bytes.Buffer)
	err = cmd.Write(buf)
	CheckError(err, "Call() Create command")
	obConn.transactions[transactionID] = name

	message := &Message{
		ChunkStreamID: CS_ID_COMMAND,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump(name)
	return obConn.conn.Send(message)

}

// Get network connect instance
func (obConn *outboundConn) Conn() Conn {
	return obConn.conn
}
