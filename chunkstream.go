// Copyright 2013, zhangpeihao All rights reserved.

package rtmp

// Chunk stream
//
// A logical channel of communication that allows flow of chunks in a
// particular direction. The chunk stream can travel from the client
// to the server and reverse.
type OutboundChunkStream struct {
	ID uint32

	lastHeader            *Header
	lastAbsoluteTimestamp uint32

	// Start at timestamp
	startAt uint32
}

type InboundChunkStream struct {
	ID uint32

	lastHeader            *Header
	lastAbsoluteTimestamp uint32

	// The unfinished incoming message
	receivedMessage *Message
}

func NewOutboundChunkStream(id uint32) *OutboundChunkStream {
	return &OutboundChunkStream{
		ID:      id,
		startAt: GetTimestamp(),
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
	timestamp := chunkStream.lastAbsoluteTimestamp + message.Timestamp
	deltaTimestamp := message.Timestamp
	if message.StreamID == 0 {
		timestamp = chunkStream.GetTimestamp()
		deltaTimestamp = timestamp - chunkStream.lastAbsoluteTimestamp
	}
	if chunkStream.lastHeader == nil {
		header.Fmt = HEADER_FMT_FULL
		if chunkStream.ID == CS_ID_PROTOCOL_CONTROL {
			// Fix timestamp
			timestamp = 0
			deltaTimestamp = 0
			header.Timestamp = 0
		} else {
			header.Timestamp = timestamp
		}
	} else {
		if chunkStream.ID == CS_ID_PROTOCOL_CONTROL {
			// Fix timestamp
			timestamp = 0
			deltaTimestamp = 0
			header.Timestamp = 0
		}

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
	chunkStream.lastHeader = header
	chunkStream.lastAbsoluteTimestamp = timestamp
	return header
}

func (chunkStream *OutboundChunkStream) GetTimestamp() uint32 {
	return GetTimestamp() - chunkStream.startAt
}
