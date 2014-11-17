// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"bytes"
	"fmt"
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
)

type InboundStreamHandler interface {
	OnPlayStart(stream InboundStream)
	OnPublishStart(stream InboundStream)
	OnReceiveAudio(stream InboundStream, on bool)
	OnReceiveVideo(stream InboundStream, on bool)
}

// Message stream:
//
// A logical channel of communication that allows the flow of
// messages.
type inboundStream struct {
	id            uint32
	streamName    string
	conn          *inboundConn
	chunkStreamID uint32
	handler       InboundStreamHandler
	bufferLength  uint32
}

// A RTMP logical stream on connection.
type InboundStream interface {
	Conn() InboundConn
	// ID
	ID() uint32
	// StreamName
	StreamName() string
	// Close
	Close()
	// Received messages
	Received(message *Message) (handlered bool)
	// Attach handler
	Attach(handler InboundStreamHandler)
	// Send audio data
	SendAudioData(data []byte, deltaTimestamp uint32) error
	// Send video data
	SendVideoData(data []byte, deltaTimestamp uint32) error
	// Send data
	SendData(dataType uint8, data []byte, deltaTimestamp uint32) error
}

func (stream *inboundStream) Conn() InboundConn {
	return stream.conn
}

// ID
func (stream *inboundStream) ID() uint32 {
	return stream.id
}

// StreamName
func (stream *inboundStream) StreamName() string {
	return stream.streamName
}

// Close
func (stream *inboundStream) Close() {
	var err error
	cmd := &Command{
		IsFlex:        true,
		Name:          "closeStream",
		TransactionID: 0,
		Objects:       make([]interface{}, 1),
	}
	cmd.Objects[0] = nil
	message := NewMessage(stream.chunkStreamID, COMMAND_AMF3, stream.id, AUTO_TIMESTAMP, nil)
	if err = cmd.Write(message.Buf); err != nil {
		return
	}
	message.Dump("closeStream")
	conn := stream.conn.Conn()
	conn.Send(message)
}

func (stream *inboundStream) Received(message *Message) bool {
	if message.Type == VIDEO_TYPE || message.Type == AUDIO_TYPE {
		return false
	}
	var err error
	if message.Type == COMMAND_AMF0 || message.Type == COMMAND_AMF3 {
		cmd := &Command{}
		if message.Type == COMMAND_AMF3 {
			cmd.IsFlex = true
			_, err = message.Buf.ReadByte()
			if err != nil {
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
					"inboundStream::Received() Read first in flex commad err:", err)
				return true
			}
		}
		cmd.Name, err = amf.ReadString(message.Buf)
		if err != nil {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
				"inboundStream::Received() AMF0 Read name err:", err)
			return true
		}
		var transactionID float64
		transactionID, err = amf.ReadDouble(message.Buf)
		if err != nil {
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
				"inboundStream::Received() AMF0 Read transactionID err:", err)
			return true
		}
		cmd.TransactionID = uint32(transactionID)
		var object interface{}
		for message.Buf.Len() > 0 {
			object, err = amf.ReadValue(message.Buf)
			if err != nil {
				logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
					"inboundStream::Received() AMF0 Read object err:", err)
				return true
			}
			cmd.Objects = append(cmd.Objects, object)
		}

		switch cmd.Name {
		case "play":
			return stream.onPlay(cmd)
		case "publish":
			return stream.onPublish(cmd)
		case "recevieAudio":
			return stream.onRecevieAudio(cmd)
		case "recevieVideo":
			return stream.onRecevieVideo(cmd)
		case "closeStream":
			return stream.onCloseStream(cmd)
		default:
			logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE, "inboundStream::Received: %+v\n", cmd)
		}

	}
	return false
}

func (stream *inboundStream) Attach(handler InboundStreamHandler) {
	stream.handler = handler
}

// Send audio data
func (stream *inboundStream) SendAudioData(data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID-4, AUDIO_TYPE, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

// Send video data
func (stream *inboundStream) SendVideoData(data []byte, deltaTimestamp uint32) (err error) {
	message := NewMessage(stream.chunkStreamID-4, VIDEO_TYPE, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

// Send data
func (stream *inboundStream) SendData(dataType uint8, data []byte, deltaTimestamp uint32) (err error) {
	var csid uint32
	switch dataType {
	case VIDEO_TYPE:
		csid = stream.chunkStreamID - 4
	case AUDIO_TYPE:
		csid = stream.chunkStreamID - 4
	default:
		csid = stream.chunkStreamID
	}
	message := NewMessage(csid, dataType, stream.id, AUTO_TIMESTAMP, data)
	message.Timestamp = deltaTimestamp
	return stream.conn.Send(message)
}

func (stream *inboundStream) onPlay(cmd *Command) bool {
	// Get stream name
	if cmd.Objects == nil || len(cmd.Objects) < 2 || cmd.Objects[1] == nil {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundStream::onPlay: command error 1! %+v\n", cmd)
		return true
	}

	if streamName, ok := cmd.Objects[1].(string); !ok {
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_WARNING,
			"inboundStream::onPlay: command error 2! %+v\n", cmd)
		return true
	} else {
		stream.streamName = streamName
	}
	// Response
	stream.conn.conn.SetChunkSize(4096)
	stream.conn.conn.SendUserControlMessage(EVENT_STREAM_BEGIN)
	stream.streamReset()
	stream.streamStart()
	stream.rtmpSampleAccess()
	stream.handler.OnPlayStart(stream)
	return true
}

func (stream *inboundStream) onPublish(cmd *Command) bool {
	return true
}
func (stream *inboundStream) onRecevieAudio(cmd *Command) bool {
	return true
}
func (stream *inboundStream) onRecevieVideo(cmd *Command) bool {
	return true
}
func (stream *inboundStream) onCloseStream(cmd *Command) bool {
	return true
}

func (stream *inboundStream) streamReset() {
	cmd := &Command{
		IsFlex:        false,
		Name:          "onStatus",
		TransactionID: 0,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = amf.Object{
		"level":       "status",
		"code":        NETSTREAM_PLAY_RESET,
		"description": fmt.Sprintf("playing and resetting %s", stream.streamName),
		"details":     stream.streamName,
	}
	buf := new(bytes.Buffer)
	err := cmd.Write(buf)
	CheckError(err, "inboundStream::streamReset() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_USER_CONTROL,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("streamReset")
	stream.conn.conn.Send(message)
}

func (stream *inboundStream) streamStart() {
	cmd := &Command{
		IsFlex:        false,
		Name:          "onStatus",
		TransactionID: 0,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = amf.Object{
		"level":       "status",
		"code":        NETSTREAM_PLAY_START,
		"description": fmt.Sprintf("Started playing %s", stream.streamName),
		"details":     stream.streamName,
	}
	buf := new(bytes.Buffer)
	err := cmd.Write(buf)
	CheckError(err, "inboundStream::streamStart() Create command")

	message := &Message{
		ChunkStreamID: CS_ID_USER_CONTROL,
		Type:          COMMAND_AMF0,
		Size:          uint32(buf.Len()),
		Buf:           buf,
	}
	message.Dump("streamStart")
	stream.conn.conn.Send(message)
}

func (stream *inboundStream) rtmpSampleAccess() {
	message := NewMessage(CS_ID_USER_CONTROL, DATA_AMF0, 0, 0, nil)
	amf.WriteString(message.Buf, "|RtmpSampleAccess")
	amf.WriteBoolean(message.Buf, false)
	amf.WriteBoolean(message.Buf, false)
	message.Dump("rtmpSampleAccess")
	stream.conn.conn.Send(message)
}
