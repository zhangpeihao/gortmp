// Copyright 2013, zhangpeihao All rights reserved.

package rtmp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/zhangpeihao/goamf"
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
}

// Connection handler
type ConnHandler interface {
	// Received message
	Received(message *Message)
	// Received command
	ReceivedCommand(command *Command)
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
	go conn.sendLoop()
	go conn.readLoop()
	return conn
}

// Send high priority message in continuous chunks
func (conn *conn) sendMessage(message *Message) {
	//	message.Dump(">>>")
	chunkStream, found := conn.outChunkStreams[message.ChunkStreamID]
	if !found {
		fmt.Printf("Can not found chunk strem id %d", message.ChunkStreamID)
		// Error
		return
	}

	header := chunkStream.NewOutboundHeader(message)
	_, err := header.Write(conn.bw)
	if err != nil {
		conn.error(err, "sendMessage write header")
	}
	if header.MessageLength > conn.outChunkSize {
		chunkStream.lastHeader = nil
		// Split into some chunk
		_, err = CopyNToNetwork(conn.bw, message.Buf, int64(conn.outChunkSize))
		if err != nil {
			conn.error(err, "sendMessage copy buffer")
		}
		remain := header.MessageLength - conn.outChunkSize
		// Type 3 chunk
		for {
			err = conn.bw.WriteByte(byte(0xc0 | byte(header.ChunkStreamID)))
			if err != nil {
				conn.error(err, "sendMessage Type 3 chunk header")
			}
			if remain > conn.outChunkSize {
				_, err = CopyNToNetwork(conn.bw, message.Buf, int64(conn.outChunkSize))
				if err != nil {
					conn.error(err, "sendMessage copy split buffer 1")
				}
				err = FlushToNetwork(conn.bw)
				if err != nil {
					conn.error(err, "sendMessage Flush 1")
				}
				remain -= conn.outChunkSize
			} else {
				_, err = CopyNToNetwork(conn.bw, message.Buf, int64(remain))
				if err != nil {
					conn.error(err, "sendMessage copy split buffer 2")
				}
				err = FlushToNetwork(conn.bw)
				if err != nil {
					conn.error(err, "sendMessage Flush 2")
				}
				break
			}
		}
	} else {
		_, err = CopyNToNetwork(conn.bw, message.Buf, int64(header.MessageLength))
		if err != nil {
			conn.error(err, "sendMessage copy buffer")
		}
	}
	err = FlushToNetwork(conn.bw)
	if err != nil {
		conn.error(err, "sendMessage Flush 3")
	}
	if conn.outChunkSizeTemp != 0 {
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
		/*
			if r := recover(); r != nil {
				if conn.err == nil {
					conn.err = r.(error)
					fmt.Println("readLoop panic:", conn.err)
				}
			}
		*/
		conn.Close()
	}()
	var found bool
	var chunkstream *InboundChunkStream
	var remain uint32
	for !conn.closed {
		// Read base header
		n, vfmt, csi, err := ReadBaseHeader(conn.br)
		CheckError(err, "ReadBaseHeader")
		conn.inBytes += uint32(n)
		// Read header
		header := &Header{}
		n, err = header.ReadHeader(conn.br, vfmt, csi)
		CheckError(err, "ReadHeader")
		conn.inBytes += uint32(n)
		// Get chunk stream
		chunkstream, found = conn.inChunkStreams[csi]
		if !found || chunkstream == nil {
			fmt.Printf("New stream: %d\n", csi)
			chunkstream = NewInboundChunkStream(csi)
			conn.inChunkStreams[csi] = chunkstream
		}
		var absoluteTimestamp uint32
		var message *Message
		switch vfmt {
		case HEADER_FMT_FULL:
			chunkstream.lastHeader = header
			absoluteTimestamp = header.Timestamp
		case HEADER_FMT_SAME_STREAM:
			// A new message with same stream ID
			if chunkstream.lastHeader == nil {
				fmt.Printf("fmt: %d, csi: %d\n", vfmt, csi)
				header.Dump("err")
			}
			header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastAbsoluteTimestamp + header.Timestamp
		case HEADER_FMT_SAME_LENGTH_AND_STREAM:
			// A new message with same stream ID, message length and message type
			if chunkstream.lastHeader == nil {
				fmt.Printf("fmt: %d, csi: %d\n", vfmt, csi)
				header.Dump("err")
			}
			header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
			header.MessageLength = chunkstream.lastHeader.MessageLength
			header.MessageTypeID = chunkstream.lastHeader.MessageTypeID
			chunkstream.lastHeader = header
			absoluteTimestamp = chunkstream.lastAbsoluteTimestamp + header.Timestamp
		case HEADER_FMT_CONTINUATION:
			// Continuation the previous unfinished message
			if chunkstream.receivedMessage == nil {
				// Some error
				fmt.Println("No unfinished message in cuntinuation fmt!")
				header.Dump("err")
			} else {
				message = chunkstream.receivedMessage
			}
			if chunkstream.lastHeader == nil {
				fmt.Printf("fmt: %d, csi: %d\n", vfmt, csi)
				header.Dump("err")
				return
			} else {
				header.MessageStreamID = chunkstream.lastHeader.MessageStreamID
				header.MessageLength = chunkstream.lastHeader.MessageLength
				header.MessageTypeID = chunkstream.lastHeader.MessageTypeID
				header.Timestamp = chunkstream.lastHeader.Timestamp
			}
			absoluteTimestamp = chunkstream.lastAbsoluteTimestamp + chunkstream.lastHeader.Timestamp
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
		chunkstream.lastAbsoluteTimestamp = absoluteTimestamp
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
						fmt.Println("!!!!!!!!!!!!!!!!")
						remain -= uint32(n64)
						continue
					}
				}
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Temporary() {
					CheckError(err, "Read data 1")
				}
			}
			// Finished message
			conn.received(message)
			chunkstream.receivedMessage = nil
		} else {
			// Unfinish
			fmt.Printf("remain: %d, conn.inChunkSize: %d\n", remain, conn.inChunkSize)
			remain = conn.inChunkSize
			for {
				// n64, err = CopyNFromNetwork(message.Buf, conn.br, int64(remain))
				n64, err = io.CopyN(message.Buf, conn.br, int64(remain))
				if err == nil {
					conn.inBytes += uint32(n64)
					if remain <= uint32(n64) {
						break
					} else {
						fmt.Println("!!!!!!!!!!!!!!!!222")
						remain -= uint32(n64)
						continue
					}
					break
				}
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Temporary() {
					CheckError(err, "Read data 2")
				}
			}
			chunkstream.receivedMessage = message
		}

		// Check window
		if conn.inBytes > (conn.inBytesPreWindow + conn.inWindowSize) {
			// Send window acknowledgement
			ackmessage := NewMessage(CS_ID_PROTOCOL_CONTROL, ACKNOWLEDGEMENT, 0, nil)
			err = binary.Write(ackmessage.Buf, binary.BigEndian, conn.inBytes)
			CheckError(err, "ACK Message write data")
			conn.inBytesPreWindow = conn.inBytes
			conn.Send(ackmessage)
		}
	}
}

func (conn *conn) error(err error, desc string) {
	fmt.Printf("Conn %s err: %s\n", desc, err.Error())
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
	fmt.Println("CreateMediaChunkStream() !!!!!!!!!!!!!!!!")
	var newChunkStreamID uint32
	conn.mediaChunkStreamIDAllocatorLocker.Lock()
	for index, occupited := range conn.mediaChunkStreamIDAllocator {
		if !occupited {
			newChunkStreamID = uint32((index+1)*6 + 2)
			fmt.Printf("index: %d, newChunkStreamID: %d\n", index, newChunkStreamID)
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
	conn.mediaChunkStreamIDAllocatorLocker.Lock()
	conn.mediaChunkStreamIDAllocator[id] = false
	conn.mediaChunkStreamIDAllocatorLocker.Unlock()
	conn.CloseChunkStream(id)
}

func (conn *conn) NewTransactionID() uint32 {
	return atomic.AddUint32(&conn.lastTransactionID, 1)
}

func (conn *conn) received(message *Message) {
	//	fmt.Print("<<< ")
	//	message.Dump("")
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
				fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE read sub type err:", err)
				return
			}

			// data size
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE read data size err:", err)
				return
			}
			dataSize = binary.BigEndian.Uint32(tmpBuf)

			// Timestamp
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE read timestamp err:", err)
				return
			}
			timestamp = binary.BigEndian.Uint32(tmpBuf)

			// Timestamp extend
			timestampExt, err = message.Buf.ReadByte()
			if err != nil {
				fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE read timestamp extend err:", err)
				return
			}
			timestamp |= (uint32(timestampExt) << 24)
			if firstAggregateTimestamp == 0 {
				firstAggregateTimestamp = timestamp
			}

			// Ignore 3 bytes
			_, err = io.ReadAtLeast(message.Buf, tmpBuf[1:], 3)
			if err != nil {
				fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE read ignore bytes err:", err)
				return
			}

			subMessage := NewMessage(message.ChunkStreamID, subType, message.StreamID, nil)
			subMessage.Timestamp = timestamp - firstAggregateTimestamp
			subMessage.IsInbound = true
			subMessage.AbsoluteTimestamp = subMessage.Timestamp + message.AbsoluteTimestamp
			// Data
			_, err = io.CopyN(subMessage.Buf, message.Buf, int64(dataSize))
			if err != nil {
				fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE copy data err:", err)
				return
			}

			// Recursion
			conn.received(subMessage)

			// Previous tag size
			if message.Buf.Len() >= 4 {
				_, err = io.ReadAtLeast(message.Buf, tmpBuf, 4)
				if err != nil {
					fmt.Println("conn::received() AGGREGATE_MESSAGE_TYPE read previous tag size err:", err)
					return
				}
				tmpBuf[0] = 0
			} else {
				break
			}
		}
	}
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
			fmt.Printf("Unkown message type %d in Protocol control chunk stream!\n", message.Type)
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
					fmt.Println("Read first in flex commad err:", err)
					return
				}
				fallthrough
			case COMMAND_AMF0:
				cmd.Name, err = amf.ReadString(message.Buf)
				if err != nil {
					fmt.Println("AMF0 Read name err:", err)
					return
				}
				transactionID, err = amf.ReadDouble(message.Buf)
				if err != nil {
					fmt.Println("AMF0 Read transactionID err:", err)
					return
				}
				cmd.TransactionID = uint32(transactionID)
				for message.Buf.Len() > 0 {
					object, err = amf.ReadValue(message.Buf)
					if err != nil {
						fmt.Println("AMF0 Read object err:", err)
						return
					}
					cmd.Objects = append(cmd.Objects, object)
				}
			default:
				fmt.Printf("Unkown message type %d in Command chunk stream!\n", message.Type)
			}
			conn.invokeCommand(cmd)
		} else {
			conn.handler.Received(message)
		}
	default:
		conn.handler.Received(message)
	}
}

func (conn *conn) invokeSetChunkSize(message *Message) {
	if err := binary.Read(message.Buf, binary.BigEndian, &conn.inChunkSize); err != nil {
		fmt.Println("invokeSetChunkSize err:", err)
	}
}

func (conn *conn) invokeAbortMessage(message *Message) {

}

func (conn *conn) invokeAcknowledgement(message *Message) {
	fmt.Printf("conn::invokeAcknowledgement(): % 2x\n", message.Buf.Bytes())
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
		fmt.Printf("invokeUserControlMessage() read event type err: %s\n", err.Error())
		return
	}
	switch eventType {
	case EVENT_STREAM_BEGIN:
		fmt.Println("EVENT_STREAM_BEGIN")
	case EVENT_STREAM_EOF:
		fmt.Println("EVENT_STREAM_EOF")
	case EVENT_STREAM_DRY:
		fmt.Println("EVENT_STREAM_DRY")
	case EVENT_SET_BUFFER_LENGTH:
		fmt.Println("EVENT_SET_BUFFER_LENGTH")
	case EVENT_STREAM_IS_RECORDED:
		fmt.Println("EVENT_STREAM_IS_RECORDED")
	case EVENT_PING_REQUEST:
		// Respond ping
		// Get server timestamp
		var serverTimestamp uint32
		err = binary.Read(message.Buf, binary.BigEndian, &serverTimestamp)
		if err != nil {
			fmt.Printf("invokeUserControlMessage() read serverTimestamp err: %s\n", err.Error())
			return
		}
		respmessage := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, nil)
		respEventType := uint16(EVENT_PING_RESPONSE)
		if err = binary.Write(respmessage.Buf, binary.BigEndian, &respEventType); err != nil {
			fmt.Println("invokeUserControlMessage write event type err:", err)
			return
		}
		if err = binary.Write(respmessage.Buf, binary.BigEndian, &serverTimestamp); err != nil {
			fmt.Println("invokeUserControlMessage write streamId err:", err)
			return
		}
		fmt.Printf("Ping response")
		conn.Send(respmessage)
	case EVENT_PING_RESPONSE:
		fmt.Println("EVENT_PING_RESPONSE")
	case EVENT_REQUEST_VERIFY:
		fmt.Println("EVENT_REQUEST_VERIFY")
	case EVENT_RESPOND_VERIFY:
		fmt.Println("EVENT_RESPOND_VERIFY")
	case EVENT_BUFFER_EMPTY:
		fmt.Println("EVENT_BUFFER_EMPTY")
	case EVENT_BUFFER_READY:
		fmt.Println("EVENT_BUFFER_READY")
	default:
		fmt.Printf("Unknown user control message :0x%x\n", eventType)
	}
}

func (conn *conn) invokeWindowAcknowledgementSize(message *Message) {
	var size uint32
	var err error
	if err = binary.Read(message.Buf, binary.BigEndian, &size); err != nil {
		fmt.Println("invokeWindowAcknowledgementSize read window size err:", err)
		return
	}
	conn.inWindowSize = size
	// Request window  acknowledgement size
	resp := &Message{
		ChunkStreamID: CS_ID_PROTOCOL_CONTROL,
		Type:          WINDOW_ACKNOWLEDGEMENT_SIZE,
		Timestamp:     GetTimestamp(),
		Buf:           new(bytes.Buffer),
	}
	if err = binary.Write(resp.Buf, binary.BigEndian, &conn.outWindowSize); err != nil {
		fmt.Println("invokeWindowAcknowledgementSize write window size err:", err)
		return
	}
	resp.Size = uint32(resp.Buf.Len())
	conn.Send(resp)
}

func (conn *conn) invokeSetPeerBandwidth(message *Message) {
	var err error
	var size uint32
	if err = binary.Read(message.Buf, binary.BigEndian, &conn.inBandwidth); err != nil {
		fmt.Println("invokeSetPeerBandwidth read window size err:", err)
		return
	}
	conn.inBandwidth = size
	var limit byte
	if limit, err = message.Buf.ReadByte(); err != nil {
		fmt.Println("invokeSetPeerBandwidth read limit err:", err)
		return
	}
	conn.inBandwidthLimit = uint8(limit)
}

func (conn *conn) invokeCommand(cmd *Command) {
	conn.handler.ReceivedCommand(cmd)
}

func (conn *conn) OutboundStreamHandler(stream *OutboundStream) {

}

func (conn *conn) SetStreamBufferSize(streamId uint32, size uint32) {
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, USER_CONTROL_MESSAGE, 0, nil)
	eventType := uint16(EVENT_SET_BUFFER_LENGTH)
	if err := binary.Write(message.Buf, binary.BigEndian, &eventType); err != nil {
		fmt.Println("SetStreamBufferSize write event type err:", err)
		return
	}
	if err := binary.Write(message.Buf, binary.BigEndian, &streamId); err != nil {
		fmt.Println("SetStreamBufferSize write streamId err:", err)
		return
	}
	if err := binary.Write(message.Buf, binary.BigEndian, &size); err != nil {
		fmt.Println("SetStreamBufferSize write size err:", err)
		return
	}
	conn.Send(message)
}

func (conn *conn) SetChunkSize(size uint32) {
	message := NewMessage(CS_ID_PROTOCOL_CONTROL, SET_CHUNK_SIZE, 0, nil)
	if err := binary.Write(message.Buf, binary.BigEndian, &size); err != nil {
		fmt.Println("SetChunkSize write event type err:", err)
		return
	}
	conn.outChunkSizeTemp = size
	conn.Send(message)
}
