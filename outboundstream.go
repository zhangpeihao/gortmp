// Copyright 2013, zhangpeihao All rights reserved.

package rtmp

import (
	"errors"
	"fmt"
	"github.com/zhangpeihao/goamf"
)

// Message stream:
//
// A logical channel of communication that allows the flow of
// messages.
type outboundStream struct {
	id            uint32
	conn          OutboundConn
	chunkStreamID uint32
}

// A RTMP logical stream on connection.
type OutboundStream interface {
	OutboundPublishStream
	OutboundPlayStream
	// ID
	ID() uint32
	// Pause
	Pause() error
	// Resume
	Resume() error
	// Close
	Close()
	// Received messages
	Received(message *Message) (handlered bool)
	// Todo: Other commands
}

// A publish stream
type OutboundPublishStream interface {
	// Publish
	Publish(name, t string) (err error)
	// Send audio data
	SendAudioData(data []byte) error
	// Send video data
	SendVideoData(data []byte) error
}

// A play stream
type OutboundPlayStream interface {
	// Play
	Play(streamName string, start, duration *uint32, reset *bool) (err error)
	// Seeks the kerframe closedst to the specified location.
	Seek(offset uint32)
}

// ID
func (stream *outboundStream) ID() uint32 {
	return stream.id
}

// Pause
func (stream *outboundStream) Pause() error {
	return errors.New("Unimplemented")
}

// Resume
func (stream *outboundStream) Resume() error {
	return errors.New("Unimplemented")
}

// Close
func (stream *outboundStream) Close() {
	// Todo: Callback to remove stream.
}

// Send audio data
func (stream *outboundStream) SendAudioData(data []byte) error {
	return errors.New("Unimplemented")
}

// Send video data
func (stream *outboundStream) SendVideoData(data []byte) error {
	return errors.New("Unimplemented")
}

// Seeks the kerframe closedst to the specified location.
func (stream *outboundStream) Seek(offset uint32) {}

func (stream *outboundStream) Publish(name, t string) (err error) {
	return errors.New("Unimplemented")
}

func (stream *outboundStream) Play(streamName string, start, duration *uint32, reset *bool) (err error) {
	conn := stream.conn.Conn()

	// Kend-die: in stream transaction ID always been 0
	// Get transaction ID
	// transactionID := conn.NewTransactionID()

	// Create play command
	cmd := &Command{
		IsFlex:        true,
		Name:          "play",
		TransactionID: 0,
		Objects:       make([]interface{}, 2),
	}
	cmd.Objects[0] = nil
	cmd.Objects[1] = streamName
	if start != nil {
		cmd.Objects = append(cmd.Objects, start)
	}
	zero := 0
	if duration != nil {
		if start == nil {
			cmd.Objects = append(cmd.Objects, &zero)
		}
		cmd.Objects = append(cmd.Objects, duration)
	}
	if reset != nil {
		if duration == nil {
			if start == nil {
				cmd.Objects = append(cmd.Objects, &zero)
			}
			cmd.Objects = append(cmd.Objects, &zero)
		}
		cmd.Objects = append(cmd.Objects, reset)
	}

	// Construct message
	message := NewMessage(stream.chunkStreamID, COMMAND_AMF3, GetTimestamp(), stream.id, nil)
	if err = cmd.Write(message.Buf); err != nil {
		return
	}
	message.Dump("play")

	return conn.Send(message)
}

func (stream *outboundStream) Received(message *Message) bool {
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
				fmt.Println("outboundStream::Received() Read first in flex commad err:", err)
				return true
			}
		}
		cmd.Name, err = amf.ReadString(message.Buf)
		if err != nil {
			fmt.Println("outboundStream::Received() AMF0 Read name err:", err)
			return true
		}
		var transactionID float64
		transactionID, err = amf.ReadDouble(message.Buf)
		if err != nil {
			fmt.Println("outboundStream::Received() AMF0 Read transactionID err:", err)
			return true
		}
		cmd.TransactionID = uint32(transactionID)
		var object interface{}
		for message.Buf.Len() > 0 {
			object, err = amf.ReadValue(message.Buf)
			if err != nil {
				fmt.Println("outboundStream::Received() AMF0 Read object err:", err)
				return true
			}
			cmd.Objects = append(cmd.Objects, object)
		}
		switch cmd.Name {
		case "onStatus":
			return stream.onStatus(cmd)
		case "onMetaData":
			return stream.onMetaData(cmd)
		case "onTimeCoordInfo":
			return stream.onTimeCoordInfo(cmd)
		}
	}
	return false
}

func (stream *outboundStream) onStatus(cmd *Command) bool {
	fmt.Printf("onStatus: %+v\n", cmd)
	code := ""
	if len(cmd.Objects) >= 2 {
		obj, ok := cmd.Objects[1].(amf.Object)
		if ok {
			value, ok := obj["code"]
			if ok {
				code, _ = value.(string)
			}
		}
	}
	switch code {
	case NETSTREAM_PLAY_START:
		fmt.Println("Play started")
		// Set buffer size
		stream.conn.Conn().SetStreamBufferSize(stream.id, 100)
	}
	return false
}

func (stream *outboundStream) onMetaData(cmd *Command) bool {
	return false
}

func (stream *outboundStream) onTimeCoordInfo(cmd *Command) bool {
	return false
}
