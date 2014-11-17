package gortmp

import (
	"bytes"
	"testing"
)

type TestBaseHeaderCase struct {
	name string
	data []byte
	fmt  uint8
	csi  uint32
}

var testBaseHeaderCases = []TestBaseHeaderCase{
	{"Chunk strea ID(64 - 319)", []byte{byte(0x00 | 0x00), 0x00}, 0x00, 64},
	{"Chunk strea ID(64 - 319)", []byte{byte(0x00 | 0x00), 0x01}, 0x00, 65},
	{"Chunk strea ID(64 - 319)", []byte{byte(0x00 | 0x00), 0xff}, 0x00, 319},
	{"Chunk strea ID(64 - 65599)", []byte{byte(0x00 | 0x01), 0x00, 0x01}, 0x00, 320},
	{"Chunk strea ID(64 - 65599)", []byte{byte(0x00 | 0x01), 0xff, 0xff}, 0x00, 65599},
	{"Chunk strea ID 2", []byte{byte(0x00 | 0x02)}, 0x00, 2},
	{"Chunk strea ID(3 - 63)", []byte{byte(0x00 | 0x03)}, 0x00, 3},
	{"Chunk strea ID(3 - 63)", []byte{byte(0x00 | 0x3f)}, 0x00, 63},
	{"fmt 1", []byte{byte(0x40 | 0x03)}, 0x01, 3},
	{"fmt 2", []byte{byte(0x80 | 0x03)}, 0x02, 3},
	{"fmt 3", []byte{byte(0xC0 | 0x03)}, 0x03, 3},
}

func TestReadBaseHeader(t *testing.T) {
	InitTestLogger()
	for _, c := range testBaseHeaderCases {
		buf := bytes.NewReader(c.data)
		n, fmt, csi, err := ReadBaseHeader(buf)
		if err != nil {
			t.Errorf("TestReadBaseHeader(%s - fmt: %d, csi: %d) error: %s", c.name, c.fmt, c.csi, err.Error())
			continue
		}
		if fmt != c.fmt || csi != c.csi || n != len(c.data) {
			t.Errorf("TestReadBaseHeader(%s - n: %d, fmt: %d, csi: %d) got: n: %d, fmt: %d, csi: %d",
				len(c.data), c.name, c.fmt, c.csi, n, fmt, csi)
		}
	}
}

type TestHeaderCase struct {
	name       string
	data       []byte
	baseHeader []byte
	header     Header
	absolute   bool
}

var testHeaderCases = []TestHeaderCase{
	{
		"Type 0",
		[]byte{
			0x00, 0x00, 0x01, // Timestamp
			0x00, 0x00, 0x02, // Message Length
			0x03,                   // Message Type ID
			0x04, 0x00, 0x00, 0x00, // Message Stream ID
		},
		[]byte{0x03},
		Header{
			Fmt:               0x00,
			ChunkStreamID:     3,
			Timestamp:         1,
			MessageLength:     2,
			MessageTypeID:     3,
			MessageStreamID:   4,
			ExtendedTimestamp: 0,
		},
		true,
	},
	{
		"Type 0 - with externed timestamp",
		[]byte{
			0xff, 0xff, 0xff, // Timestamp
			0x00, 0x00, 0x02, // Message Length
			0x03,                   // Message Type ID
			0x04, 0x00, 0x00, 0x00, // Message Stream ID
			0x10, 0x00, 0x00, 0x00, // Externed timestamp
		},
		[]byte{0x03},
		Header{
			Fmt:               0x00,
			ChunkStreamID:     3,
			Timestamp:         0xffffff,
			MessageLength:     2,
			MessageTypeID:     3,
			MessageStreamID:   4,
			ExtendedTimestamp: 0x10000000,
		},
		true,
	},
	{
		"Type 1",
		[]byte{
			0x00, 0x00, 0x11, // Timestamp
			0x00, 0x00, 0x12, // Message Length
			0x13, // Message Type ID
		},
		[]byte{0x43},
		Header{
			Fmt:               0x01,
			ChunkStreamID:     3,
			Timestamp:         0x11,
			MessageLength:     0x12,
			MessageTypeID:     0x13,
			MessageStreamID:   0x04,
			ExtendedTimestamp: 0,
		},
		false,
	},
	{
		"Type 1 - with externed timestamp",
		[]byte{
			0xff, 0xff, 0xff, // Timestamp
			0x00, 0x00, 0x12, // Message Length
			0x13,                   // Message Type ID
			0x11, 0x00, 0x00, 0x00, // Externed timestamp
		},
		[]byte{0x43},
		Header{
			Fmt:               0x01,
			ChunkStreamID:     3,
			Timestamp:         0xffffff,
			MessageLength:     0x12,
			MessageTypeID:     0x13,
			MessageStreamID:   0x04,
			ExtendedTimestamp: 0x11000000,
		},
		false,
	},
	{
		"Type 2",
		[]byte{
			0x00, 0x00, 0x21, // Timestamp
		},
		[]byte{0x83},
		Header{
			Fmt:               0x02,
			ChunkStreamID:     3,
			Timestamp:         0x21,
			MessageLength:     0x12,
			MessageTypeID:     0x13,
			MessageStreamID:   0x04,
			ExtendedTimestamp: 0,
		},
		false,
	},
	{
		"Type 3",
		[]byte{}, // No data
		[]byte{0xc3},
		Header{
			Fmt:               0x03,
			ChunkStreamID:     3,
			Timestamp:         0x21,
			MessageLength:     0x12,
			MessageTypeID:     0x13,
			MessageStreamID:   0x04,
			ExtendedTimestamp: 0,
		},
		false,
	},
	{
		"Type 2 - with externed timestamp",
		[]byte{
			0xff, 0xff, 0xff, // Timestamp
			0x21, 0x00, 0x00, 0x00, // Externed timestamp
		},
		[]byte{0x83},
		Header{
			Fmt:               0x02,
			ChunkStreamID:     3,
			Timestamp:         0xffffff,
			MessageLength:     0x12,
			MessageTypeID:     0x13,
			MessageStreamID:   0x04,
			ExtendedTimestamp: 0x21000000,
		},
		false,
	},
}

func TestReadHeader(t *testing.T) {
	InitTestLogger()
	header := &Header{}
	for _, c := range testHeaderCases {
		buf := bytes.NewReader(c.data)
		n, err := header.ReadHeader(buf, c.header.Fmt, c.header.ChunkStreamID, nil)
		if err != nil {
			t.Errorf("TestReadHeader(%s)\n\t%v\nerror: %s", c.name, c.header, err.Error())
			continue
		}
		if *header != c.header {
			t.Errorf("TestReadHeader(%s)\n\texpect: %v\n\tgot:   %v", c.name, c.header, *header)
		}
		if n != len(c.data) {
			t.Errorf("TestReadHeader(%s)\n\texpect n: %d\n\tgot n:   %d", c.name, len(c.data), n)
		}
	}
}

func TestWriteHeader(t *testing.T) {
	InitTestLogger()
	for _, c := range testHeaderCases {
		buf := new(bytes.Buffer)
		n, err := c.header.Write(buf)
		if err != nil {
			t.Errorf("TestWriteHeader(%s)\n\t%v\nerror: %s", c.name, c.header, err.Error())
			continue
		}
		expect := append(c.baseHeader, c.data...)
		if n != len(expect) {
			t.Errorf("TestWriteHeader(%s)\n\texpect length: %d\n\tn:%d", c.name, len(expect), n)
		}
		got := buf.Bytes()
		if !bytes.Equal(expect, got) {
			t.Errorf("TestWriteHeader(%s)\n\texpect: % x\n\tgot:   % x", c.name, expect, got)
		}
	}
}
