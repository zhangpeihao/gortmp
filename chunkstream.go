// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"github.com/zhangpeihao/log"
)

// Chunk stream
//
// A logical channel of communication that allows flow of chunks in a
// particular direction. The chunk stream can travel from the client
// to the server and reverse.
type OutboundChunkStream struct {
	ID uint32

	lastHeader               *Header
	lastOutAbsoluteTimestamp uint32
	lastInAbsoluteTimestamp  uint32

	// Start at timestamp
	startAt uint32
}

type InboundChunkStream struct {
	ID uint32

	lastHeader               *Header
	lastOutAbsoluteTimestamp uint32
	lastInAbsoluteTimestamp  uint32

	// The unfinished incoming message
	receivedMessage *Message
}

func NewOutboundChunkStream(id uint32) *OutboundChunkStream {
	return &OutboundChunkStream{
		ID: id,
	}
}

func NewInboundChunkStream(id uint32) *InboundChunkStream {
	return &InboundChunkStream{
		ID: id,
	}
}

func (chunkStream *OutboundChunkStream) NewOutboundHeader(message *Message) *Header {
	header := &Header{
		ChunkStreamID:   chunkStream.ID,
		MessageLength:   uint32(message.Buf.Len()),
		MessageTypeID:   message.Type,
		MessageStreamID: message.StreamID,
	}
	timestamp := message.Timestamp
	if timestamp == AUTO_TIMESTAMP {
		timestamp = chunkStream.GetTimestamp()
		message.Timestamp = timestamp
		message.AbsoluteTimestamp = timestamp
	}
	deltaTimestamp := uint32(0)
	if chunkStream.lastOutAbsoluteTimestamp < message.Timestamp {
		deltaTimestamp = message.Timestamp - chunkStream.lastOutAbsoluteTimestamp
	}
	if chunkStream.lastHeader == nil {
		header.Fmt = HEADER_FMT_FULL
		header.Timestamp = timestamp
	} else {

		if header.MessageStreamID == chunkStream.lastHeader.MessageStreamID {
			if header.MessageTypeID == chunkStream.lastHeader.MessageTypeID &&
				header.MessageLength == chunkStream.lastHeader.MessageLength {
				switch chunkStream.lastHeader.Fmt {
				case HEADER_FMT_FULL:
					header.Fmt = HEADER_FMT_SAME_LENGTH_AND_STREAM
					header.Timestamp = deltaTimestamp
				case HEADER_FMT_SAME_STREAM:
					fallthrough
				case HEADER_FMT_SAME_LENGTH_AND_STREAM:
					fallthrough
				case HEADER_FMT_CONTINUATION:
					if chunkStream.lastHeader.Timestamp == deltaTimestamp {
						header.Fmt = HEADER_FMT_CONTINUATION
					} else {
						header.Fmt = HEADER_FMT_SAME_LENGTH_AND_STREAM
						header.Timestamp = deltaTimestamp
					}
				}
			} else {
				header.Fmt = HEADER_FMT_SAME_STREAM
				header.Timestamp = deltaTimestamp
			}
		} else {
			header.Fmt = HEADER_FMT_FULL
			header.Timestamp = timestamp
		}
	}
	// Check extended timestamp
	if header.Timestamp >= 0xffffff {
		header.ExtendedTimestamp = message.Timestamp
		header.Timestamp = 0xffffff
	} else {
		header.ExtendedTimestamp = 0
	}
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
		"OutboundChunkStream::NewOutboundHeader() header: %+v\n", header)
	chunkStream.lastHeader = header
	chunkStream.lastOutAbsoluteTimestamp = timestamp
	return header
}

func (chunkStream *OutboundChunkStream) GetTimestamp() uint32 {
	if chunkStream.startAt == uint32(0) {
		chunkStream.startAt = GetTimestamp()
		return uint32(0)
	}
	return GetTimestamp() - chunkStream.startAt
}
