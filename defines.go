// Copyright 2013, zhangpeihao All rights reserved.

// RTMP protocol golang implementation
package gortmp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

var DefaultObjectEncoding uint = amf.AMF0
var logger *log.Logger = log.NewStderrLogger()
var logHandler = logger.LoggerModule(RTMP_LOG_NAME)

const (
	RTMP_LOG_NAME = "rtmp"
)

// Chunk Message Header - "fmt" field values
const (
	HEADER_FMT_FULL                   = 0x00
	HEADER_FMT_SAME_STREAM            = 0x01
	HEADER_FMT_SAME_LENGTH_AND_STREAM = 0x02
	HEADER_FMT_CONTINUATION           = 0x03
)

// Result codes
const (
	RESULT_CONNECT_OK            = "NetConnection.Connect.Success"
	RESULT_CONNECT_REJECTED      = "NetConnection.Connect.Rejected"
	RESULT_CONNECT_OK_DESC       = "Connection successed."
	RESULT_CONNECT_REJECTED_DESC = "[ AccessManager.Reject ] : [ code=400 ] : "
	NETSTREAM_PLAY_START         = "NetStream.Play.Start"
	NETSTREAM_PLAY_RESET         = "NetStream.Play.Reset"
	NETSTREAM_PUBLISH_START      = "NetStream.Publish.Start"
)

// Chunk stream ID
const (
	CS_ID_PROTOCOL_CONTROL = uint32(2)
	CS_ID_COMMAND          = uint32(3)
	CS_ID_USER_CONTROL     = uint32(4)
)

// Message type
const (
	// Set Chunk Size
	//
	// Protocol control message 1, Set Chunk Size, is used to notify the
	// peer a new maximum chunk size to use.

	// The value of the chunk size is carried as 4-byte message payload. A
	// default value exists for chunk size, but if the sender wants to
	// change this value it notifies the peer about it through this
	// protocol message. For example, a client wants to send 131 bytes of
	// data and the chunk size is at its default value of 128. So every
	// message from the client gets split into two chunks. The client can
	// choose to change the chunk size to 131 so that every message get
	// split into two chunks. The client MUST send this protocol message to
	// the server to notify that the chunk size is set to 131 bytes.
	// The maximum chunk size can be 65536 bytes. Chunk size is maintained
	// independently for server to client communication and client to server
	// communication.
	//
	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                          chunk size (4 bytes)                 |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// Figure 2 Pay load for the protocol message ‘Set Chunk Size’
	//
	// chunk size: 32 bits
	//   This field holds the new chunk size, which will be used for all
	//   future chunks sent by this chunk stream.
	SET_CHUNK_SIZE = uint8(1)

	// Abort Message
	//
	// Protocol control message 2, Abort Message, is used to notify the peer
	// if it is waiting for chunks to complete a message, then to discard
	// the partially received message over a chunk stream and abort
	// processing of that message. The peer receives the chunk stream ID of
	// the message to be discarded as payload of this protocol message. This
	// message is sent when the sender has sent part of a message, but wants
	// to tell the receiver that the rest of the message will not be sent.
	//
	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                        chunk stream id (4 bytes)              |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// Figure 3 Pay load for the protocol message ‘Abort Message’.
	//
	//
	// chunk stream ID: 32 bits
	//   This field holds the chunk stream ID, whose message is to be
	//   discarded.
	ABORT_MESSAGE = uint8(2)

	// Acknowledgement
	//
	// The client or the server sends the acknowledgment to the peer after
	// receiving bytes equal to the window size. The window size is the
	// maximum number of bytes that the sender sends without receiving
	// acknowledgment from the receiver. The server sends the window size to
	// the client after application connects. This message specifies the
	// sequence number, which is the number of the bytes received so far.
	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                        sequence number (4 bytes)              |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// Figure 4 Pay load for the protocol message ‘Acknowledgement’.
	//
	// sequence number: 32 bits
	//   This field holds the number of bytes received so far.
	ACKNOWLEDGEMENT = uint8(3)

	// User Control Message
	//
	// The client or the server sends this message to notify the peer about
	// the user control events. This message carries Event type and Event
	// data.
	// +------------------------------+-------------------------
	// |     Event Type ( 2- bytes ) | Event Data
	// +------------------------------+-------------------------
	// Figure 5 Pay load for the ‘User Control Message’.
	//
	//
	// The first 2 bytes of the message data are used to identify the Event
	// type. Event type is followed by Event data. Size of Event data field
	// is variable.
	USER_CONTROL_MESSAGE = uint8(4)

	// Window Acknowledgement Size
	//
	// The client or the server sends this message to inform the peer which
	// window size to use when sending acknowledgment. For example, a server
	// expects acknowledgment from the client every time the server sends
	// bytes equivalent to the window size. The server updates the client
	// about its window size after successful processing of a connect
	// request from the client.
	//
	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                   Acknowledgement Window size (4 bytes)       |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// Figure 6 Pay load for ‘Window Acknowledgement Size’.
	WINDOW_ACKNOWLEDGEMENT_SIZE = uint8(5)

	// Set Peer Bandwidth
	//
	// The client or the server sends this message to update the output
	// bandwidth of the peer. The output bandwidth value is the same as the
	// window size for the peer. The peer sends ‘Window Acknowledgement
	// Size’ back if its present window size is different from the one
	// received in the message.
	//  0                   1                   2                   3
	//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// |                   Acknowledgement Window size                 |
	// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	// | Limit type    |
	// +-+-+-+-+-+-+-+-+
	// Figure 7 Pay load for ‘Set Peer Bandwidth’
	//
	// The sender can mark this message hard (0), soft (1), or dynamic (2)
	// using the Limit type field. In a hard (0) request, the peer must send
	// the data in the provided bandwidth. In a soft (1) request, the
	// bandwidth is at the discretion of the peer and the sender can limit
	// the bandwidth. In a dynamic (2) request, the bandwidth can be hard or
	// soft.
	SET_PEER_BANDWIDTH = uint8(6)

	// Audio message
	//
	// The client or the server sends this message to send audio data to the
	// peer. The message type value of 8 is reserved for audio messages.
	AUDIO_TYPE = uint8(8)

	// Video message
	//
	// The client or the server sends this message to send video data to the
	// peer. The message type value of 9 is reserved for video messages.
	// These messages are large and can delay the sending of other type of
	// messages. To avoid such a situation, the video message is assigned
	// the lowest priority.
	VIDEO_TYPE = uint8(9)

	// Aggregate message
	//
	// An aggregate message is a single message that contains a list of sub-
	// messages. The message type value of 22 is reserved for aggregate
	// messages.
	AGGREGATE_MESSAGE_TYPE = uint8(22)

	// Shared object message
	//
	// A shared object is a Flash object (a collection of name value pairs)
	// that are in synchronization across multiple clients, instances, and
	// so on. The message types kMsgContainer=19 for AMF0 and
	// kMsgContainerEx=16 for AMF3 are reserved for shared object events.
	// Each message can contain multiple events.
	SHARED_OBJECT_AMF0 = uint8(19)
	SHARED_OBJECT_AMF3 = uint8(16)

	// Data message
	//
	// The client or the server sends this message to send Metadata or any
	// user data to the peer. Metadata includes details about the
	// data(audio, video etc.) like creation time, duration, theme and so
	// on. These messages have been assigned message type value of 18 for
	// AMF0 and message type value of 15 for AMF3.
	DATA_AMF0 = uint8(18)
	DATA_AMF3 = uint8(15)

	// Command message
	//
	// Command messages carry the AMF-encoded commands between the client
	// and the server. These messages have been assigned message type value
	// of 20 for AMF0 encoding and message type value of 17 for AMF3
	// encoding. These messages are sent to perform some operations like
	// connect, createStream, publish, play, pause on the peer. Command
	// messages like onstatus, result etc. are used to inform the sender
	// about the status of the requested commands. A command message
	// consists of command name, transaction ID, and command object that
	// contains related parameters. A client or a server can request Remote
	// Procedure Calls (RPC) over streams that are communicated using the
	// command messages to the peer.
	COMMAND_AMF0 = uint8(20)
	COMMAND_AMF3 = uint8(17) // Keng-die!!! Just ignore one byte before AMF0.
)

const (
	EVENT_STREAM_BEGIN       = uint16(0)
	EVENT_STREAM_EOF         = uint16(1)
	EVENT_STREAM_DRY         = uint16(2)
	EVENT_SET_BUFFER_LENGTH  = uint16(3)
	EVENT_STREAM_IS_RECORDED = uint16(4)
	EVENT_PING_REQUEST       = uint16(6)
	EVENT_PING_RESPONSE      = uint16(7)
	EVENT_REQUEST_VERIFY     = uint16(0x1a)
	EVENT_RESPOND_VERIFY     = uint16(0x1b)
	EVENT_BUFFER_EMPTY       = uint16(0x1f)
	EVENT_BUFFER_READY       = uint16(0x20)
)

const (
	BINDWIDTH_LIMIT_HARD    = uint8(0)
	BINDWIDTH_LIMIT_SOFT    = uint8(1)
	BINDWIDTH_LIMIT_DYNAMIC = uint8(2)
)

var (
	//	FLASH_PLAYER_VERSION = []byte{0x0A, 0x00, 0x2D, 0x02}
	FLASH_PLAYER_VERSION = []byte{0x09, 0x00, 0x7C, 0x02}
	//FLASH_PLAYER_VERSION = []byte{0x80, 0x00, 0x07, 0x02}
	//FLASH_PLAYER_VERSION_STRING = "LNX 10,0,32,18"
	FLASH_PLAYER_VERSION_STRING = "LNX 9,0,124,2"
	//FLASH_PLAYER_VERSION_STRING = "WIN 11,5,502,146"
	SWF_URL_STRING     = "http://localhost/1.swf"
	PAGE_URL_STRING    = "http://localhost/1.html"
	MIN_BUFFER_LENGTH  = uint32(256)
	FMS_VERSION        = []byte{0x04, 0x05, 0x00, 0x01}
	FMS_VERSION_STRING = "4,5,0,297"
)

const (
	MAX_TIMESTAMP                       = uint32(2000000000)
	AUTO_TIMESTAMP                      = uint32(0XFFFFFFFF)
	DEFAULT_HIGH_PRIORITY_BUFFER_SIZE   = 2048
	DEFAULT_MIDDLE_PRIORITY_BUFFER_SIZE = 128
	DEFAULT_LOW_PRIORITY_BUFFER_SIZE    = 64
	DEFAULT_CHUNK_SIZE                  = uint32(128)
	DEFAULT_WINDOW_SIZE                 = 2500000
	DEFAULT_CAPABILITIES                = float64(15)
	DEFAULT_AUDIO_CODECS                = float64(4071)
	DEFAULT_VIDEO_CODECS                = float64(252)
	FMS_CAPBILITIES                     = uint32(255)
	FMS_MODE                            = uint32(2)
	SET_PEER_BANDWIDTH_HARD             = byte(0)
	SET_PEER_BANDWIDTH_SOFT             = byte(1)
	SET_PEER_BANDWIDTH_DYNAMIC          = byte(2)
)

type Writer interface {
	Write(p []byte) (nn int, err error)
	WriteByte(c byte) error
}

type Reader interface {
	Read(p []byte) (n int, err error)
	ReadByte() (c byte, err error)
}

type RtmpURL struct {
	protocol     string
	host         string
	port         uint16
	app          string
	instanceName string
}

func init() {
	logger = log.NewStderrLogger()
	logHandler = logger.LoggerModule(RTMP_LOG_NAME)
}

// Init log module
// Must initialize log first.
func InitLogger(l *log.Logger) {
	logger = l
	logHandler = logger.LoggerModule(RTMP_LOG_NAME)
}

// Check error
//
// If error panic
func CheckError(err error, name string) {
	if err != nil {
		panic(errors.New(fmt.Sprintf("%s: %s", name, err.Error())))
	}
}

// Parse url
//
// To connect to Flash Media Server, pass the URI of the application on the server.
// Use the following syntax (items in brackets are optional):
//
// protocol://host[:port]/[appname[/instanceName]]
func ParseURL(url string) (rtmpURL RtmpURL, err error) {
	s1 := strings.SplitN(url, "://", 2)
	if len(s1) != 2 {
		err = errors.New(fmt.Sprintf("Parse url %s error. url invalid.", url))
		return
	}
	rtmpURL.protocol = strings.ToLower(s1[0])
	s1 = strings.SplitN(s1[1], "/", 2)
	if len(s1) != 2 {
		err = errors.New(fmt.Sprintf("Parse url %s error. no app!", url))
		return
	}
	s2 := strings.SplitN(s1[0], ":", 2)
	if len(s2) == 2 {
		var port int
		port, err = strconv.Atoi(s2[1])
		if err != nil {
			err = errors.New(fmt.Sprintf("Parse url %s error. port error: %s.", url, err.Error()))
			return
		}
		if port > 65535 || port <= 0 {
			err = errors.New(fmt.Sprintf("Parse url %s error. port error: %d.", url, port))
			return
		}
		rtmpURL.port = uint16(port)
	} else {
		rtmpURL.port = 1935
	}
	if len(s2[0]) == 0 {
		err = errors.New(fmt.Sprintf("Parse url %s error. host is empty.", url))
		return
	}
	rtmpURL.host = s2[0]

	s2 = strings.SplitN(s1[1], "/", 2)
	rtmpURL.app = s2[0]
	if len(s2) == 2 {
		rtmpURL.instanceName = s2[1]
	}
	return
	/*
		if len(s1) == 3 {
			if strings.HasPrefix(s1[1], "//") && len(s1[1]) > 2 {
				rtmpURL.host = s1[1][2:]
				if len(rtmpURL.host) == 0 {
					err = errors.New(fmt.Sprintf("Parse url %s error. Host is empty.", url))
					return
				}
			} else {
				err = errors.New(fmt.Sprintf("Parse url %s error. Host invalid.", url))
				return
			}
			fmt.Printf("s1: %v\n", s1)
			s2 := strings.SplitN(s1[2], "/", 3)
			var port int
			port, err = strconv.Atoi(s2[0])
			if err != nil {
				err = errors.New(fmt.Sprintf("Parse url %s error. port error: %s.", url, err.Error()))
				return
			}
			if port > 65535 || port <= 0 {
				err = errors.New(fmt.Sprintf("Parse url %s error. port error: %d.", url, port))
				return
			}
			rtmpURL.port = uint16(port)
			if len(s2) > 1 {
				rtmpURL.app = s2[1]
			}
			if len(s2) > 2 {
				rtmpURL.instanceName = s2[2]
			}
		} else {
			if len(s1) < 2 {
				err = errors.New(fmt.Sprintf("Parse url %s error. url invalid.", url))
				return
			}
			// Default port
			rtmpURL.port = 1935
			if strings.HasPrefix(s1[1], "//") && len(s1[1]) > 2 {
				s2 := strings.SplitN(s1[1][2:], "/", 3)
				rtmpURL.host = s2[0]
				if len(rtmpURL.host) == 0 {
					err = errors.New(fmt.Sprintf("Parse url %s error. Host is empty.", url))
					return
				}
				if len(s2) > 1 {
					rtmpURL.app = s2[1]
				}
				if len(s2) > 2 {
					rtmpURL.instanceName = s2[2]
				}
			} else {
				err = errors.New(fmt.Sprintf("Parse url %s error. Host invalid.", url))
				return
			}
		}
		return
	*/
}

func (rtmpUrl *RtmpURL) App() string {
	if len(rtmpUrl.instanceName) == 0 {
		return rtmpUrl.app
	}
	return rtmpUrl.app + "/" + rtmpUrl.instanceName
}

// Dump buffer
func DumpBuffer(name string, data []byte, ind int) {
	if logger.ModuleLevelCheck(logHandler, log.LOG_LEVEL_DEBUG) {
		var logstring string
		logstring = fmt.Sprintf("Buffer(%s):\n", name)
		for i := 0; i < len(data); i++ {
			logstring += fmt.Sprintf("%02x ", data[i])
			switch (i + 1 + ind) % 16 {
			case 0:
				logstring += fmt.Sprintln("")
			case 8:
				logstring += fmt.Sprint(" ")
			}
		}
		logstring += fmt.Sprintln("")
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG, logstring)
	}
}

// Get timestamp
func GetTimestamp() uint32 {
	//return uint32(0)
	return uint32(time.Now().UnixNano()/int64(1000000)) % MAX_TIMESTAMP
}

// Read byte from network
func ReadByteFromNetwork(r Reader) (b byte, err error) {
	retry := 1
	for {
		b, err = r.ReadByte()
		if err == nil {
			return
		}
		netErr, ok := err.(net.Error)
		if !ok {
			return
		}
		if !netErr.Temporary() {
			return
		}
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG,
			"ReadByteFromNetwork block")
		if retry < 16 {
			retry = retry * 2
		}
		time.Sleep(time.Duration(retry*100) * time.Millisecond)
	}
	return
}

// Read bytes from network
func ReadAtLeastFromNetwork(r Reader, buf []byte, min int) (n int, err error) {
	retry := 1
	for {
		n, err = io.ReadAtLeast(r, buf, min)
		if err == nil {
			return
		}
		netErr, ok := err.(net.Error)
		if !ok {
			return
		}
		if !netErr.Temporary() {
			return
		}
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG,
			"ReadAtLeastFromNetwork !!!!!!!!!!!!!!!!!!")
		if retry < 16 {
			retry = retry * 2
		}
		time.Sleep(time.Duration(retry*100) * time.Millisecond)
	}
	return
}

// Copy bytes from network
func CopyNFromNetwork(dst Writer, src Reader, n int64) (written int64, err error) {
	// return io.CopyN(dst, src, n)

	buf := make([]byte, 4096)
	for written < n {
		l := len(buf)
		if d := n - written; d < int64(l) {
			l = int(d)
		}
		nr, er := ReadAtLeastFromNetwork(src, buf[0:l], l)
		if er != nil {
			err = er
			break
		}
		if nr == l {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		} else {
			err = io.ErrShortBuffer
		}
	}
	return
}

func WriteToNetwork(w Writer, data []byte) (written int, err error) {
	length := len(data)
	var n int
	retry := 1
	for written < length {
		n, err = w.Write(data[written:])
		if err == nil {
			written += int(n)
			continue
		}
		netErr, ok := err.(net.Error)
		if !ok {
			return
		}
		if !netErr.Temporary() {
			return
		}
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG,
			"WriteToNetwork !!!!!!!!!!!!!!!!!!")
		if retry < 16 {
			retry = retry * 2
		}
		time.Sleep(time.Duration(retry*500) * time.Millisecond)
	}
	return

}

// Copy bytes to network
func CopyNToNetwork(dst Writer, src Reader, n int64) (written int64, err error) {
	// return io.CopyN(dst, src, n)

	buf := make([]byte, 4096)
	for written < n {
		l := len(buf)
		if d := n - written; d < int64(l) {
			l = int(d)
		}
		nr, er := io.ReadAtLeast(src, buf[0:l], l)
		if nr > 0 {
			nw, ew := WriteToNetwork(dst, buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			err = er
			break
		}
	}
	return
}

func FlushToNetwork(w *bufio.Writer) (err error) {
	retry := 1
	for {
		err = w.Flush()
		if err == nil {
			return
		}
		netErr, ok := err.(net.Error)
		if !ok {
			return
		}
		if !netErr.Temporary() {
			return
		}
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG,
			"FlushToNetwork !!!!!!!!!!!!!!!!!!")
		if retry < 16 {
			retry = retry * 2
		}
		time.Sleep(time.Duration(retry*500) * time.Millisecond)
	}
	return
}
