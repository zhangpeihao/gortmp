// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"encoding/binary"
	"errors"
	"github.com/zhangpeihao/log"
)

// RTMP Chunk Header
//
// The header is broken down into three parts:
//
// | Basic header|Chunk Msg Header|Extended Time Stamp|   Chunk Data |
//
// Chunk basic header: 1 to 3 bytes
//
// This field encodes the chunk stream ID and the chunk type. Chunk
// type determines the format of the encoded message header. The
// length depends entirely on the chunk stream ID, which is a
// variable-length field.
//
// Chunk message header: 0, 3, 7, or 11 bytes
//
// This field encodes information about the message being sent
// (whether in whole or in part). The length can be determined using
// the chunk type specified in the chunk header.
//
// Extended timestamp: 0 or 4 bytes
//
// This field MUST be sent when the normal timsestamp is set to
// 0xffffff, it MUST NOT be sent if the normal timestamp is set to
// anything else. So for values less than 0xffffff the normal
// timestamp field SHOULD be used in which case the extended timestamp
// MUST NOT be present. For values greater than or equal to 0xffffff
// the normal timestamp field MUST NOT be used and MUST be set to
// 0xffffff and the extended timestamp MUST be sent.
type Header struct {
	// Basic Header
	Fmt           uint8
	ChunkStreamID uint32

	// Chunk Message Header
	Timestamp       uint32
	MessageLength   uint32
	MessageTypeID   uint8
	MessageStreamID uint32

	// Extended Timestamp
	ExtendedTimestamp uint32
}

// Read Base Header from io.Reader
// High level protocol can use chunk stream ID to query the previous header instance.
func ReadBaseHeader(rbuf Reader) (n int, fmt uint8, csi uint32, err error) {
	var b byte
	b, err = ReadByteFromNetwork(rbuf)
	if err != nil {
		return
	}
	n = 1
	fmt = uint8(b >> 6)
	b = b & 0x3f
	switch b {
	case 0:
		// Chunk stream IDs 64-319 can be encoded in the 2-byte version of this
		// field. ID is computed as (the second byte + 64).
		b, err = ReadByteFromNetwork(rbuf)
		if err != nil {
			return
		}
		n += 1
		csi = uint32(64) + uint32(b)
	case 1:
		// Chunk stream IDs 64-65599 can be encoded in the 3-byte version of
		// this field. ID is computed as ((the third byte)*256 + the second byte
		// + 64).
		b, err = ReadByteFromNetwork(rbuf)
		if err != nil {
			return
		}
		n += 1
		csi = uint32(64) + uint32(b)
		b, err = ReadByteFromNetwork(rbuf)
		if err != nil {
			return
		}
		n += 1
		csi += uint32(b) * 256
	default:
		// Chunk stream IDs 2-63 can be encoded in the 1-byte version of this
		// field.
		csi = uint32(b)
	}
	return
}

// Read new chunk stream header from io.Reader
func (header *Header) ReadHeader(rbuf Reader, vfmt uint8, csi uint32, lastheader *Header) (n int, err error) {
	header.Fmt = vfmt
	header.ChunkStreamID = csi
	var b byte
	tmpBuf := make([]byte, 4)
	switch header.Fmt {
	case HEADER_FMT_FULL:
		// Chunks of Type 0 are 11 bytes long. This type MUST be used at the
		// start of a chunk stream, and whenever the stream timestamp goes
		// backward (e.g., because of a backward seek).
		//
		//  0                   1                   2                   3
		//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		// |                   timestamp                   |message length |
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		// |     message length (cont)     |message type id| msg stream id |
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		// |           message stream id (cont)            |
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		//       Figure 9 Chunk Message Header – Type 0
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf[1:], 3)
		if err != nil {
			return
		}
		n += 3
		header.Timestamp = binary.BigEndian.Uint32(tmpBuf)
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf[1:], 3)
		if err != nil {
			return
		}
		n += 3
		header.MessageLength = binary.BigEndian.Uint32(tmpBuf)
		b, err = ReadByteFromNetwork(rbuf)
		if err != nil {
			return
		}
		n += 1
		header.MessageTypeID = uint8(b)
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf, 4)
		if err != nil {
			return
		}
		n += 4
		header.MessageStreamID = binary.LittleEndian.Uint32(tmpBuf)
	case HEADER_FMT_SAME_STREAM:
		// Chunks of Type 1 are 7 bytes long. The message stream ID is not
		// included; this chunk takes the same stream ID as the preceding chunk.
		// Streams with variable-sized messages (for example, many video
		// formats) SHOULD use this format for the first chunk of each new
		// message after the first.
		//
		//  0                   1                   2                   3
		//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		// |                timestamp delta                |message length |
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		// |     message length (cont)     |message type id|
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		//       Figure 10 Chunk Message Header – Type 1
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf[1:], 3)
		if err != nil {
			return
		}
		n += 3
		header.Timestamp = binary.BigEndian.Uint32(tmpBuf)
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf[1:], 3)
		if err != nil {
			return
		}
		n += 3
		header.MessageLength = binary.BigEndian.Uint32(tmpBuf)
		b, err = ReadByteFromNetwork(rbuf)
		if err != nil {
			return
		}
		n += 1
		header.MessageTypeID = uint8(b)

	case HEADER_FMT_SAME_LENGTH_AND_STREAM:
		// Chunks of Type 2 are 3 bytes long. Neither the stream ID nor the
		// message length is included; this chunk has the same stream ID and
		// message length as the preceding chunk. Streams with constant-sized
		// messages (for example, some audio and data formats) SHOULD use this
		// format for the first chunk of each message after the first.
		//
		//  0                   1                   2
		//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		// |                timestamp delta                |
		// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
		//       Figure 11 Chunk Message Header – Type 2
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf[1:], 3)
		if err != nil {
			return
		}
		n += 3
		header.Timestamp = binary.BigEndian.Uint32(tmpBuf)

	case HEADER_FMT_CONTINUATION:
		// Chunks of Type 3 have no header. Stream ID, message length and
		// timestamp delta are not present; chunks of this type take values from
		// the preceding chunk. When a single message is split into chunks, all
		// chunks of a message except the first one, SHOULD use this type. Refer
		// to example 2 in section 6.2.2. Stream consisting of messages of
		// exactly the same size, stream ID and spacing in time SHOULD use this
		// type for all chunks after chunk of Type 2. Refer to example 1 in
		// section 6.2.1. If the delta between the first message and the second
		// message is same as the time stamp of first message, then chunk of
		// type 3 would immediately follow the chunk of type 0 as there is no
		// need for a chunk of type 2 to register the delta. If Type 3 chunk
		// follows a Type 0 chunk, then timestamp delta for this Type 3 chunk is
		// the same as the timestamp of Type 0 chunk.
	}
	// [Extended Timestamp]
	// This field is transmitted only when the normal time stamp in the
	// chunk message header is set to 0x00ffffff. If normal time stamp is
	// set to any value less than 0x00ffffff, this field MUST NOT be
	// present. This field MUST NOT be present if the timestamp field is not
	// present. Type 3 chunks MUST NOT have this field.
	// !!!!!! crtmpserver set this field in Type 3 !!!!!!
	// Todo: Test with FMS
	if (header.Fmt != HEADER_FMT_CONTINUATION && header.Timestamp >= 0xffffff) ||
		(header.Fmt == HEADER_FMT_CONTINUATION && lastheader != nil && lastheader.ExtendedTimestamp > 0) {
		_, err = ReadAtLeastFromNetwork(rbuf, tmpBuf, 4)
		if err != nil {
			return
		}
		n += 4
		header.ExtendedTimestamp = binary.BigEndian.Uint32(tmpBuf)
		logger.ModulePrintf(logHandler, log.LOG_LEVEL_TRACE,
			"Extened timestamp: %d, timestamp: %d, fmt: %d\n", header.ExtendedTimestamp, header.Timestamp, header.Fmt)
		header.Dump("Extended timestamp")
	} else {
		header.ExtendedTimestamp = 0
	}
	return
}

// Encode header into io.Writer
func (header *Header) Write(wbuf Writer) (n int, err error) {
	// Write fmt & Chunk stream ID
	switch {
	case header.ChunkStreamID <= 63:
		err = wbuf.WriteByte(byte((header.Fmt << 6) | byte(header.ChunkStreamID)))
		if err != nil {
			return
		}
		n += 1
	case header.ChunkStreamID <= 319:
		err = wbuf.WriteByte(header.Fmt << 6)
		if err != nil {
			return
		}
		n += 1
		err = wbuf.WriteByte(byte(header.ChunkStreamID - 64))
		if err != nil {
			return
		}
		n += 1
	case header.ChunkStreamID <= 65599:
		err = wbuf.WriteByte((header.Fmt << 6) | 0x01)
		if err != nil {
			return
		}
		n += 1
		tmp := uint16(header.ChunkStreamID - 64)
		err = binary.Write(wbuf, binary.BigEndian, &tmp)
		if err != nil {
			return
		}
		n += 2
	default:
		return n, errors.New("Unsupport chunk stream ID large then 65599")
	}
	tmpBuf := make([]byte, 4)
	var m int
	switch header.Fmt {
	case HEADER_FMT_FULL:
		// Timestamp
		binary.BigEndian.PutUint32(tmpBuf, header.Timestamp)
		m, err = wbuf.Write(tmpBuf[1:])
		if err != nil {
			return
		}
		n += m
		// Message Length
		binary.BigEndian.PutUint32(tmpBuf, header.MessageLength)
		m, err = wbuf.Write(tmpBuf[1:])
		if err != nil {
			return
		}
		n += m
		// Message Type
		err = wbuf.WriteByte(header.MessageTypeID)
		if err != nil {
			return
		}
		n += 1
		// Message Stream ID
		err = binary.Write(wbuf, binary.LittleEndian, &(header.MessageStreamID))
		if err != nil {
			return
		}
		n += 4
	case HEADER_FMT_SAME_STREAM:
		// Timestamp
		binary.BigEndian.PutUint32(tmpBuf, header.Timestamp)
		m, err = wbuf.Write(tmpBuf[1:])
		if err != nil {
			return
		}
		n += m
		// Message Length
		binary.BigEndian.PutUint32(tmpBuf, header.MessageLength)
		m, err = wbuf.Write(tmpBuf[1:])
		if err != nil {
			return
		}
		n += m
		// Message Type
		err = wbuf.WriteByte(header.MessageTypeID)
		if err != nil {
			return
		}
		n += 1
	case HEADER_FMT_SAME_LENGTH_AND_STREAM:
		// Timestamp
		binary.BigEndian.PutUint32(tmpBuf, header.Timestamp)
		m, err = wbuf.Write(tmpBuf[1:])
		if err != nil {
			return
		}
		n += m
	case HEADER_FMT_CONTINUATION:
	}

	// Type 3 chunks MUST NOT have Extended timestamp????
	// Todo: Test with FMS
	// if header.Timestamp >= 0xffffff && header.Fmt != HEADER_FMT_CONTINUATION {
	if header.Timestamp >= 0xffffff {
		// Extended Timestamp
		err = binary.Write(wbuf, binary.BigEndian, &(header.ExtendedTimestamp))
		if err != nil {
			return
		}
		n += 4
	}
	return
}

func (header *Header) RealTimestamp() uint32 {
	if header.Timestamp >= 0xffffff {
		return header.ExtendedTimestamp
	}
	return header.Timestamp
}

func (header *Header) Dump(name string) {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
		"Header(%s){Fmt: %d, ChunkStreamID: %d, Timestamp: %d, MessageLength: %d, MessageTypeID: %d, MessageStreamID: %d, ExtendedTimestamp: %d}\n", name,
		header.Fmt, header.ChunkStreamID, header.Timestamp, header.MessageLength,
		header.MessageTypeID, header.MessageStreamID, header.ExtendedTimestamp)
}
