// Copyright 2013, zhangpeihao All rights reserved.

// RTMP protocol golang implementation
package rtmp

// Chunk Message Header - "fmt" field values
const (
	HEADER_FMT_FULL                   = 0x00
	HEADER_FMT_SAME_STREAM            = 0x01
	HEADER_FMT_SAME_LENGTH_AND_STREAM = 0x02
	HEADER_FMT_CONTINUATION           = 0x03
)
