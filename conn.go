// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn
//
// Common connection functions
type Conn interface {
	Close()
	Send(message *Message) error
	CreateChunkStream(ID uint32) (*OutboundChunkStream, error)
	CloseChunkStream(ID uint32)
	NewTransactionID() uint32
	CreateMediaChunkStream() (*OutboundChunkStream, error)
	CloseMediaChunkStream(id uint32)
	SetStreamBufferSize(streamId uint32, size uint32)
	OutboundChunkStream(id uint32) (chunkStream *OutboundChunkStream, found bool)
	InboundChunkStream(id uint32) (chunkStream *InboundChunkStream, found bool)
	SetWindowAcknowledgementSize()
	SetPeerBandwidth(peerBandwidth uint32, limitType byte)
	SetChunkSize(chunkSize uint32)
	SendUserControlMessage(eventId uint16)
}

// Connection handler
type ConnHandler interface {
	// Received message
	OnReceived(conn Conn, message *Message)
	// Received command
	OnReceivedRtmpCommand(conn Conn, command *Command)
	// Connection closed
	OnClosed(conn Conn)
}

// conn
//
// To maintain all chunk streams in one network connection.
type conn struct {
	// Chunk streams
	outChunkStreams map[uint32]*OutboundChunkStream
	inChunkStreams  map[uint32]*InboundChunkStream

	// High-priority send message buffer.
	// Protocol control messages are sent with highest priority.
	highPriorityMessageQueue  chan *Message
	highPriorityMessage       *Message
	highPriorityMessageOffset int

	// Middle-priority send message buffer.
	middlePriorityMessageQueue  chan *Message
	middlePriorityMessage       *Message
	middlePriorityMessageOffset int

	// Low-priority send message buffer.
	// the video message is assigned the lowest priority.
	lowPriorityMessageQueue  chan *Message
	lowPriorityMessage       *Message
	lowPriorityMessageOffset int

	// Chunk size
	inChunkSize      uint32
	outChunkSize     uint32
	outChunkSizeTemp uint32

	// Bytes counter(For window ack)
	inBytes  uint32
	outBytes uint32

	// Previous window acknowledgement inbytes
	inBytesPreWindow uint32

	// Window size
	inWindowSize  uint32
	outWindowSize uint32

	// Bandwidth
	inBandwidth  uint32
	outBandwidth uint32

	// Bandwidth Limit
	inBandwidthLimit  uint8
	outBandwidthLimit uint8

	// Media chunk stream ID
	mediaChunkStreamIDAllocator       []bool
	mediaChunkStreamIDAllocatorLocker sync.Mutex

	// Closed
	closed bool

	// Handler
	handler ConnHandler

	// Network connection
	c  net.Conn
	br *bufio.Reader
	bw *bufio.Writer

	// Last transaction ID
	lastTransactionID uint32

	// Error
	err error
}

// Create new connection
func NewConn(c net.Conn, br *bufio.Reader, bw *bufio.Writer, handler ConnHandler, maxChannelNumber int) Conn {
	conn := &conn{
		c:                           c,
		br:                          br,
		bw:                          bw,
		outChunkStreams:             make(map[uint32]*OutboundChunkStream),
		inChunkStreams:              make(map[uint32]*InboundChunkStream),
		highPriorityMessageQueue:    make(chan *Message, DEFAULT_HIGH_PRIORITY_BUFFER_SIZE),
		middlePriorityMessageQueue:  make(chan *Message, DEFAULT_MIDDLE_PRIORITY_BUFFER_SIZE),
		lowPriorityMessageQueue:     make(chan *Message, DEFAULT_LOW_PRIORITY_BUFFER_SIZE),
		inChunkSize:                 DEFAULT_CHUNK_SIZE,
		outChunkSize:                DEFAULT_CHUNK_SIZE,
		inWindowSize:                DEFAULT_WINDOW_SIZE,
		outWindowSize:               DEFAULT_WINDOW_SIZE,
		inBandwidth:                 DEFAULT_WINDOW_SIZE,
		outBandwidth:                DEFAULT_WINDOW_SIZE,
		inBandwidthLimit:            BINDWIDTH_LIMIT_DYNAMIC,
		outBandwidthLimit:           BINDWIDTH_LIMIT_DYNAMIC,
		handler:                     handler,
		mediaChunkStreamIDAllocator: make([]bool, maxChannelNumber),
	}
	// Create "Protocol control chunk stream"
	conn.outChunkStreams[CS_ID_PROTOCOL_CONTROL] = NewOutboundChunkStream(CS_ID_PROTOCOL_CONTROL)
	// Create "Command message chunk stream"
	conn.outChunkStreams[CS_ID_COMMAND] = NewOutboundChunkStream(CS_ID_COMMAND)
	// Create "User control chunk stream"
	conn.outChunkStreams[CS_ID_USER_CONTROL] = NewOutboundChunkStream(CS_ID_USER_CONTROL)
	go conn.sendLoop()
	go conn.readLoop()
	return conn
}

// Send high priority message in continuous chunks
func (conn *conn) sendMessage(message *Message) {
	chunkStream, found := conn.outChunkStreams[message.ChunkStreamID]
	if !found {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"Can not found chunk strem id %d", message.ChunkStreamID)
		// Error
		return
	}

	//	message.Dump(">>>")
	header := chunkStream.NewOutboundHeader(message)
	_, err := header.Write(conn.bw)
	if err != nil {
		conn.error(err, "sendMessage write header")
		return
	}
	//	header.Dump(">>>")
	if header.MessageLength > conn.outChunkSize {
		//		chunkStream.lastHeader = nil
		// Split into some chunk
		_, err = CopyNToNetwork(conn.bw, message.Buf, int64(conn.outChunkSize))
		if err != nil {
			conn.error(err, "sendMessage copy buffer")
			return
		}
		remain := header.MessageLength - conn.outChunkSize
		// Type 3 chunk
		for {
			err = conn.bw.WriteByte(byte(0xc0 | byte(header.ChunkStreamID)))
			if err != nil {
				conn.error(err, "sendMessage Type 3 chunk header")
				return
			}
			if remain > conn.outChunkSize {
				_, err = CopyNToNetwork(conn.bw, message.Buf, int64(conn.outChunkSize))
				if err != nil {
					conn.error(err, "sendMessage copy split buffer 1")
					return
				}
				remain -= conn.outChunkSize
			} else {
				_, err = CopyNToNetwork(conn.bw, message.Buf, int64(remain))
				if err != nil {
					conn.error(err, "sendMessage copy split buffer 2")
					return
				}
				break
			}
		}
	} else {
		_, err = CopyNToNetwork(conn.bw, message.Buf, int64(header.MessageLength))
		if err != nil {
			conn.error(err, "sendMessage copy buffer")
			return
		}
	}
	err = FlushToNetwork(conn.bw)
	if err != nil {
		conn.error(err, "sendMessage Flush 3")
		return
	}
	if message.ChunkStreamID == CS_ID_PROTOCOL_CONTROL &&
		message.Type == SET_CHUNK_SIZE &&
		conn.outChunkSizeTemp != 0 {
		// Set chunk size
		conn.outChunkSize = conn.outChunkSizeTemp
		conn.outChunkSizeTemp = 0
	}
}

func (conn *conn) checkAndSendHighPriorityMessage() {
	for len(conn.highPriorityMessageQueue) > 0 {
		message := <-conn.highPriorityMessageQueue
		conn.sendMessage(message)
	}
}

// send loop
func (conn *conn) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
			if conn.err == nil {
				conn.err = r.(error)
			}
		}
		conn.Close()
	}()
	for !conn.closed {
		select {
		case message := <-conn.highPriorityMessageQueue:
			// Send all high priority messages
			conn.sendMessage(message)
		case message := <-conn.middlePriorityMessageQueue:
			// Send one middle priority messages
			conn.sendMessage(message)
			conn.checkAndSendHighPriorityMessage()
		case message := <-conn.lowPriorityMessageQueue:
			// Check high priority message queue first
			conn.checkAndSendHighPriorityMessage()
			conn.sendMessage(message)
		case <-time.After(time.Second):
			// Check close
		}
	}
}

// read loop
func (conn *conn) readLoop() {

	defer func() {

		if r := recover(); r != nil {
			if conn.err == nil {
				conn.err = r.(error)
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
					"readLoop panic:", conn.err)
			}
		}
		conn.Close()
		conn.handler.OnClosed(conn)
	}()

	var found bool
	var chunkstream *InboundChunkStream
	var remain uint32
	for !conn.closed {
		// Read base header
		n, vfmt, csi, err := ReadBaseHeader(conn.br)
		CheckError(err, "ReadBaseHeader")
		conn.inBytes += uint32(n)
		// Get chunk stream
		chunkstream, found = conn.inChunkStreams[csi]
		if !found || chunkstream == nil {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE, "New stream 1 csi: %d, fmt: %d\n", csi, vfmt)
			chunkstream = NewInboundChunkStream(csi)
			conn.inChunkStreams[csi] = chunkstream
		}
		// Read header
		header := &Header{}
		n, err = header.ReadHeader(conn.br, vfmt, csi, chunkstream.lastHeader)
		CheckError(err, "ReadHeader")
		if !found {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE, "New stream 2 csi: %d, fmt: %d, header: %+v\n", csi, vfmt, header)
		}
		conn.inBytes += uint32(n)
		var absoluteTimestamp uint32
		var message *Message
		switch vfmt {
		case HEADER_FMT_FULL:
			chunkstream.lastHeader = header
			absoluteTimestamp = header.Timestamp
		case HEADER_FMT_SAME_STREAM:
			// A new message with same stream ID
			if chunkstream.lastHeader == nil {
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
					"A new message with fmt: %d, csi: %d\n", vfmt, csi)
				header.Dump("err")
			} else {
				header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
			}
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastInAbsoluteTimestamp + header.Timestamp
		case HEADER_FMT_SAME_LENGTH_AND_STREAM:
			// A new message with same stream ID, message length and message type
			if chunkstream.lastHeader == nil {
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
					"A new message with fmt: %d, csi: %d\n", vfmt, csi)
				header.Dump("err")
			}
			header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
			header.MessageLength = chunkstream.lastHeader.MessageLength
			header.MessageTypeID = chunkstream.lastHeader.MessageTypeID
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastInAbsoluteTimestamp + header.Timestamp
		case HEADER_FMT_CONTINUATION:
			if chunkstream.receivedMessage != nil {
				// Continuation the previous unfinished message
				message = chunkstream.receivedMessage
			}
			if chunkstream.lastHeader == nil {
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
					"A new message with fmt: %d, csi: %d\n", vfmt, csi)
				header.Dump("err")
			} else {
				header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
				header.MessageLength = chunkstream.lastHeader.MessageLength
				header.MessageTypeID = chunkstream.lastHeader.MessageTypeID
				header.Timestamp = chunkstream.lastHeader.Timestamp
			}
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastInAbsoluteTimestamp
		}
		if message == nil {
			// New message
			message = &Message{
				ChunkStreamID:     csi,
				Type:              header.MessageTypeID,
				Timestamp:         header.RealTimestamp(),
				Size:              header.MessageLength,
				StreamID:          header.MessageStreamID,
				Buf:               new(bytes.Buffer),
				IsInbound:         true,
				AbsoluteTimestamp: absoluteTimestamp,
			}
		}
		chunkstream.lastInAbsoluteTimestamp = absoluteTimestamp
		// Read data
		remain = message.Remain()
		var n64 int64
		if remain <= conn.inChunkSize {
			// One chunk message
			for {
				// n64, err = CopyNFromNetwork(message.Buf, conn.br, int64(remain))
				n64, err = io.CopyN(message.Buf, conn.br, int64(remain))
				if err == nil {
					conn.inBytes += uint32(n64)
					if remain <= uint32(n64) {
						break
					} else {
						remain -= uint32(n64)
						logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
							"Message continue copy remain: %d\n", remain)
						continue
					}
				}
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Temporary() {
					CheckError(err, "Read data 1")
				}
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
					"Message copy blocked!\n")
			}
			// Finished message
			conn.received(message)
			chunkstream.receivedMessage = nil
		} else {
			// Unfinish
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
				"Unfinish message(remain: %d, chunksize: %d)\n", remain, conn.inChunkSize)

			remain = conn.inChunkSize
			for {
				// n64, err = CopyNFromNetwork(message.Buf, conn.br, int64(remain))
				n64, err = io.CopyN(message.Buf, conn.br, int64(remain))
				if err == nil {
					conn.inBytes += uint32(n64)
					if remain <= uint32(n64) {
						break
					} else {
						remain -= uint32(n64)
						logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
							"Unfinish message continue copy remain: %d\n", remain)
						continue
					}
					break
				}
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Temporary() {
					CheckError(err, "Read data 2")
				}
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
					"Unfinish message copy blocked!\n")
			}
			chunkstream.receivedMessage = message
		}

		// Check window
		if conn.inBytes > (conn.inBytesPreWindow + conn.inWindowSize) {
			// Send window acknowledgement
			ackmessage := NewMessage(CS_ID_PROTOCOL_CONTROL, ACKNOWLEDGEMENT, 0, absoluteTimestamp+1, nil)
			err = binary.Write(ackmessage.Buf, binary.BigEndian, conn.inBytes)
			CheckError(err, "ACK Message write data")
			conn.inBytesPreWindow = conn.inBytes
			conn.Send(ackmessage)
		}
	}
}

func (conn *conn) error(err error, desc string) {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"Conn %s err: %s\n", desc, err.Error())
	if conn.err == nil {
		conn.err = err
	}
	conn.Close()
}

func (conn *conn) Close() {
	//	panic(errors.New("Closed"))

	conn.closed = true
	conn.c.Close()
}

// Send a message by channel
func (conn *conn) Send(message *Message) error {
	csiType := (message.ChunkStreamID % 6)
	if csiType == CS_ID_PROTOCOL_CONTROL || csiType == CS_ID_COMMAND {
		// High priority
		conn.highPriorityMessageQueue <- message
		return nil
	}
	if message.Type == VIDEO_TYPE {
		// Low priority
		conn.lowPriorityMessageQueue <- message
		return nil
	}
	conn.middlePriorityMessageQueue <- message
	return nil
}

func (conn *conn) CreateChunkStream(id uint32) (*OutboundChunkStream, error) {
	chunkStream, found := conn.outChunkStreams[id]
	if found {
		return nil, errors.New("Chunk stream existed")
	}
	chunkStream = NewOutboundChunkStream(id)
	conn.outChunkStreams[id] = chunkStream
	return chunkStream, nil
}

func (conn *conn) CloseChunkStream(id uint32) {
	delete(conn.outChunkStreams, id)
}

func (conn *conn) CreateMediaChunkStream() (*OutboundChunkStream, error) {
	var newChunkStreamID uint32
	conn.mediaChunkStreamIDAllocatorLocker.Lock()
	for index, occupited := range conn.mediaChunkStreamIDAllocator {
		if !occupited {
			newChunkStreamID = uint32((index+1)*6 + 2)
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
				"index: %d, newChunkStreamID: %d\n", index, newChunkStreamID)
			// since allocate a newChunkStreamID, why not set the cocupited to true
			conn.mediaChunkStreamIDAllocator[index] = true
			break
		}
	}
	conn.mediaChunkStreamIDAllocatorLocker.Unlock()
	if newChunkStreamID == 0 {
		return nil, errors.New("No more chunk stream ID to allocate")
	}
	chunkSteam, err := conn.CreateChunkStream(newChunkStreamID)
	if err != nil {
		conn.CloseMediaChunkStream(newChunkStreamID)
		return nil, err
	}
	return chunkSteam, nil
}

func (conn *conn) OutboundChunkStream(id uint32) (chunkStream *OutboundChunkStream, found bool) {
	chunkStream, found = conn.outChunkStreams[id]
	return
}

func (conn *conn) InboundChunkStream(id uint32) (chunkStream *InboundChunkStream, found bool) {
	chunkStream, found = conn.inChunkStreams[id]
	return
}

func (conn *conn) CloseMediaChunkStream(id uint32) {
	// and the id is not the index of Allocator slice
	index := (id - 2) / 6 - 1
	conn.mediaChunkStreamIDAllocatorLocker.Lock()
	conn.mediaChunkStreamIDAllocator[index] = false
	conn.mediaChunkStreamIDAllocatorLocker.Unlock()
	conn.CloseChunkStream(id)
}

func (conn *conn) NewTransactionID() uint32 {
	return atomic.AddUint32(&conn.lastTransactionID, 1)
}

func (conn *conn) received(message *Message) {
	message.Dump("<<<")
	tmpBuf := make([]byte, 4)
	var err error
	var subType byte
	var dataSize uint32
	var timestamp uint32
	var timestampExt byte
	if message.Type == AGGREGATE_MESSAGE_TYPE {
		// Byte stream order
		// Sub message type 1 byte
		// Data size 3 bytes, big endian
		// Timestamp 3 bytes
		// Timestamp extend 1 byte,  result = (result >>> 8) | ((result & 0x000000ff) << 24);
		// 3 bytes ignored
		// Data
		// Previous tag size 4 bytes
		var firstAggregateTimestamp uint32
		for message.Buf.Len() > 0 {
			// Sub type
			subType, err = message.Buf.ReadByte()
			if err != nil {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE read sub type err:", err)
				return
			}

			// data size
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE read data size err:", err)
				return
			}
			dataSize = binary.BigEndian.Uint32(tmpBuf)

			// Timestamp
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE read timestamp err:", err)
				return
			}
			timestamp = binary.BigEndian.Uint32(tmpBuf)

			// Timestamp extend
			timestampExt, err = message.Buf.ReadByte()
			if err != nil {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE read timestamp extend err:", err)
				return
			}
			timestamp |= (uint32(timestampExt) << 24)
			if firstAggregateTimestamp == 0 {
				firstAggregateTimestamp = timestamp
			}

			// Ignore 3 bytes
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE read ignore bytes err:", err)
				return
			}

			subMessage := NewMessage(message.ChunkStreamID, subType, message.StreamID, 0, nil)
			subMessage.Timestamp = 0
			subMessage.IsInbound = true
			subMessage.Size = dataSize
			subMessage.AbsoluteTimestamp = message.AbsoluteTimestamp
			// Data
			_, err = io.CopyN(subMessage.Buf, message.Buf, int64(dataSize))
			if err != nil {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE copy data err:", err)
				return
			}

			// Recursion
			conn.received(subMessage)

			// Previous tag size
			if message.Buf.Len() >= 4 {
				_, err = io.ReadAtLeast(message.Buf, tmpBuf, 4)
				if err != nil {
					logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
						"conn::received() AGGREGATE_MESSAGE_TYPE read previous tag size err:", err)
					return
				}
				tmpBuf[0] = 0
			} else {
				logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
					"conn::received() AGGREGATE_MESSAGE_TYPE miss previous tag size")
				break
			}
		}
	} else {
		switch message.ChunkStreamID {
		case CS_ID_PROTOCOL_CONTROL:
			switch message.Type {
			case SET_CHUNK_SIZE:
				conn.invokeSetChunkSize(message)
			case ABORT_MESSAGE:
				conn.invokeAbortMessage(message)
			case ACKNOWLEDGEMENT:
				conn.invokeAcknowledgement(message)
			case USER_CONTROL_MESSAGE:
				conn.invokeUserControlMessage(message)
			case WINDOW_ACKNOWLEDGEMENT_SIZE:
				conn.invokeWindowAcknowledgementSize(message)
			case SET_PEER_BANDWIDTH:
				conn.invokeSetPeerBandwidth(message)
			default:
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
					"Unkown message type %d in Protocol control chunk stream!\n", message.Type)
			}
		case CS_ID_COMMAND:
			if message.StreamID == 0 {
				cmd := &Command{}
				var err error
				var transactionID float64
				var object interface{}
				switch message.Type {
				case COMMAND_AMF3:
					cmd.IsFlex = true
					_, err = message.Buf.ReadByte()
					if err != nil {
						logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
							"Read first in flex commad err:", err)
						return
					}
					fallthrough
				case COMMAND_AMF0:
					cmd.Name, err = amf.ReadString(message.Buf)
					if err != nil {
						logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
							"AMF0 Read name err:", err)
						return
					}
					transactionID, err = amf.ReadDouble(message.Buf)
					if err != nil {
						logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
							"AMF0 Read transactionID err:", err)
						return
					}
					cmd.TransactionID = uint32(transactionID)
					for message.Buf.Len() > 0 {
						object, err = amf.ReadValue(message.Buf)
						if err != nil {
							logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
								"AMF0 Read object err:", err)
							return
						}
						cmd.Objects = append(cmd.Objects, object)
					}
				default:
					logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
						"Unkown message type %d in Command chunk stream!\n", message.Type)
				}
				conn.invokeCommand(cmd)
			} else {
				conn.handler.OnReceived(conn, message)
			}
		default:
			conn.handler.OnReceived(conn, message)
		}
	}
}

func (conn *conn) invokeSetChunkSize(message *Message) {
	if err := binary.Read(message.Buf, binary.BigEndian, &conn.inChunkSize); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::invokeSetChunkSize err:", err)
	}
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn::invokeSetChunkSize() conn.inChunkSize = %d\n", conn.inChunkSize)
}

func (conn *conn) invokeAbortMessage(message *Message) {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"conn::invokeAbortMessage()")
}

func (conn *conn) invokeAcknowledgement(message *Message) {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn::invokeAcknowledgement(): % 2x\n", message.Buf.Bytes())
}

// User Control Message
//
// The client or the server sends this message to notify the peer about
// the user control events. This message carries Event type and Event
// data.
// +------------------------------+-------------------------
// |     Event Type ( 2- bytes )  | Event Data
// +------------------------------+-------------------------
// Figure 5 Pay load for the ‘User Control Message’.
//
//
// The first 2 bytes of the message data are used to identify the Event
// type. Event type is followed by Event data. Size of Event data field
// is variable.
//
//
// The client or the server sends this message to notify the peer about
// the user control events. For information about the message format,
// refer to the User Control Messages section in the RTMP Message
// Foramts draft.
//
// The following user control event types are supported:
// +---------------+--------------------------------------------------+
// |     Event     |                   Description                    |
// +---------------+--------------------------------------------------+
// |Stream Begin   | The server sends this event to notify the client |
// |        (=0)   | that a stream has become functional and can be   |
// |               | used for communication. By default, this event   |
// |               | is sent on ID 0 after the application connect    |
// |               | command is successfully received from the        |
// |               | client. The event data is 4-byte and represents  |
// |               | the stream ID of the stream that became          |
// |               | functional.                                      |
// +---------------+--------------------------------------------------+
// | Stream EOF    | The server sends this event to notify the client |
// |        (=1)   | that the playback of data is over as requested   |
// |               | on this stream. No more data is sent without     |
// |               | issuing additional commands. The client discards |
// |               | the messages received for the stream. The        |
// |               | 4 bytes of event data represent the ID of the    |
// |               | stream on which playback has ended.              |
// +---------------+--------------------------------------------------+
// | StreamDry     | The server sends this event to notify the client |
// |      (=2)     | that there is no more data on the stream. If the |
// |               | server does not detect any message for a time    |
// |               | period, it can notify the subscribed clients     |
// |               | that the stream is dry. The 4 bytes of event     |
// |               | data represent the stream ID of the dry stream.  |
// +---------------+--------------------------------------------------+
// | SetBuffer     | The client sends this event to inform the server |
// | Length (=3)   | of the buffer size (in milliseconds) that is     |
// |               | used to buffer any data coming over a stream.    |
// |               | This event is sent before the server starts      |
// |               | processing the stream. The first 4 bytes of the  |
// |               | event data represent the stream ID and the next  |
// |               | 4 bytes represent the buffer length, in          |
// |               | milliseconds.                                    |
// +---------------+--------------------------------------------------+
// | StreamIs      | The server sends this event to notify the client |
// | Recorded (=4) | that the stream is a recorded stream. The        |
// |               | 4 bytes event data represent the stream ID of    |
// |               | the recorded stream.                             |
// +---------------+--------------------------------------------------+
// | PingRequest   | The server sends this event to test whether the  |
// |       (=6)    | client is reachable. Event data is a 4-byte      |
// |               | timestamp, representing the local server time    |
// |               | when the server dispatched the command. The      |
// |               | client responds with kMsgPingResponse on         |
// |               | receiving kMsgPingRequest.                       |
// +---------------+--------------------------------------------------+
// | PingResponse  | The client sends this event to the server in     |
// |        (=7)   | response to the ping request. The event data is  |
// |               | a 4-byte timestamp, which was received with the  |
// |               | kMsgPingRequest request.                         |
// +---------------+--------------------------------------------------+
func (conn *conn) invokeUserControlMessage(message *Message) {
	var eventType uint16
	err := binary.Read(message.Buf, binary.BigEndian, &eventType)
	if err != nil {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"conn::invokeUserControlMessage() read event type err: %s\n", err.Error())
		return
	}
	switch eventType {
	case EVENT_STREAM_BEGIN:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_STREAM_BEGIN")
	case EVENT_STREAM_EOF:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_STREAM_EOF")
	case EVENT_STREAM_DRY:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_STREAM_DRY")
	case EVENT_SET_BUFFER_LENGTH:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_SET_BUFFER_LENGTH")
	case EVENT_STREAM_IS_RECORDED:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_STREAM_IS_RECORDED")
	case EVENT_PING_REQUEST:
		// Respond ping
		// Get server timestamp
		var serverTimestamp uint32
		err = binary.Read(message.Buf, binary.BigEndian, &serverTimestamp)
		if err != nil {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
				"conn::invokeUserControlMessage() read serverTimestamp err: %s\n", err.Error())
			return
		}
		respmessage := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, message.Timestamp+1, nil)
		respEventType := uint16(EVENT_PING_RESPONSE)
		if err = binary.Write(respmessage.Buf, binary.BigEndian, &respEventType); err != nil {
			logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
				"conn::invokeUserControlMessage() write event type err:", err)
			return
		}
		if err = binary.Write(respmessage.Buf, binary.BigEndian, &serverTimestamp); err != nil {
			logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
				"conn::invokeUserControlMessage() write streamId err:", err)
			return
		}
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() Ping response")
		conn.Send(respmessage)
	case EVENT_PING_RESPONSE:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_PING_RESPONSE")
	case EVENT_REQUEST_VERIFY:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_REQUEST_VERIFY")
	case EVENT_RESPOND_VERIFY:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_RESPOND_VERIFY")
	case EVENT_BUFFER_EMPTY:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_BUFFER_EMPTY")
	case EVENT_BUFFER_READY:
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() EVENT_BUFFER_READY")
	default:
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE, "conn::invokeUserControlMessage() Unknown user control message :0x%x\n", eventType)
	}
}

func (conn *conn) invokeWindowAcknowledgementSize(message *Message) {
	var size uint32
	var err error
	if err = binary.Read(message.Buf, binary.BigEndian, &size); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::invokeWindowAcknowledgementSize read window size err:", err)
		return
	}
	conn.inWindowSize = size
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn::invokeWindowAcknowledgementSize() conn.inWindowSize = %d\n", conn.inWindowSize)
}

func (conn *conn) invokeSetPeerBandwidth(message *Message) {
	var err error
	var size uint32
	if err = binary.Read(message.Buf, binary.BigEndian, &conn.inBandwidth); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::invokeSetPeerBandwidth read window size err:", err)
		return
	}
	conn.inBandwidth = size
	var limit byte
	if limit, err = message.Buf.ReadByte(); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::invokeSetPeerBandwidth read limit err:", err)
		return
	}
	conn.inBandwidthLimit = uint8(limit)
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn.inBandwidthLimit = %d/n", conn.inBandwidthLimit)
}

func (conn *conn) invokeCommand(cmd *Command) {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"conn::invokeCommand()")
	conn.handler.OnReceivedRtmpCommand(conn, cmd)
}

func (conn *conn) SetStreamBufferSize(streamId uint32, size uint32) {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn::SetStreamBufferSize(streamId: %d, size: %d)\n", streamId, size)
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, 1, nil)
	eventType := uint16(EVENT_SET_BUFFER_LENGTH)
	if err := binary.Write(message.Buf, binary.BigEndian, &eventType); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetStreamBufferSize write event type err:", err)
		return
	}
	if err := binary.Write(message.Buf, binary.BigEndian, &streamId); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetStreamBufferSize write streamId err:", err)
		return
	}
	if err := binary.Write(message.Buf, binary.BigEndian, &size); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetStreamBufferSize write size err:", err)
		return
	}
	conn.Send(message)
}

func (conn *conn) SetChunkSize(size uint32) {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn::SetChunkSize(size: %d)\n", size)
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, SET_CHUNK_SIZE, 0, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &size); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetChunkSize write event type err:", err)
		return
	}
	conn.outChunkSizeTemp = size
	conn.Send(message)
}

func (conn *conn) SetWindowAcknowledgementSize() {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"conn::SetWindowAcknowledgementSize")
	// Request window acknowledgement size
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, WINDOW_ACKNOWLEDGEMENT_SIZE, 0, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &conn.outWindowSize); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetWindowAcknowledgementSize write window size err:", err)
		return
	}
	message.Size = uint32(message.Buf.Len())
	conn.Send(message)
}
func (conn *conn) SetPeerBandwidth(peerBandwidth uint32, limitType byte) {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"conn::SetPeerBandwidth")
	// Request window acknowledgement size
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, SET_PEER_BANDWIDTH, 0, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &peerBandwidth); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetPeerBandwidth write peerBandwidth err:", err)
		return
	}
	if err := message.Buf.WriteByte(limitType); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SetPeerBandwidth write limitType err:", err)
		return
	}
	message.Size = uint32(message.Buf.Len())
	conn.Send(message)
}

func (conn *conn) SendUserControlMessage(eventId uint16) {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
		"conn::SendUserControlMessage")
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &eventId); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"conn::SendUserControlMessage write event type err:", err)
		return
	}
	conn.Send(message)
}
