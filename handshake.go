// Copyright 2013, zhangpeihao All rights reserved.

package rtmp

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"
)

const (
	RTMP_SIG_SIZE          = 1536
	RTMP_LARGE_HEADER_SIZE = 12
	SHA256_DIGEST_LENGTH   = 32
	RTMP_DEFAULT_CHUNKSIZE = 128
)

var (
	GENUINE_FMS_KEY = []byte{
		0x47, 0x65, 0x6e, 0x75, 0x69, 0x6e, 0x65, 0x20,
		0x41, 0x64, 0x6f, 0x62, 0x65, 0x20, 0x46, 0x6c,
		0x61, 0x73, 0x68, 0x20, 0x4d, 0x65, 0x64, 0x69,
		0x61, 0x20, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72,
		0x20, 0x30, 0x30, 0x31, // Genuine Adobe Flash Media Server 001
		0xf0, 0xee, 0xc2, 0x4a, 0x80, 0x68, 0xbe, 0xe8,
		0x2e, 0x00, 0xd0, 0xd1, 0x02, 0x9e, 0x7e, 0x57,
		0x6e, 0xec, 0x5d, 0x2d, 0x29, 0x80, 0x6f, 0xab,
		0x93, 0xb8, 0xe6, 0x36, 0xcf, 0xeb, 0x31, 0xae,
	}
	GENUINE_FP_KEY = []byte{
		0x47, 0x65, 0x6E, 0x75, 0x69, 0x6E, 0x65, 0x20,
		0x41, 0x64, 0x6F, 0x62, 0x65, 0x20, 0x46, 0x6C,
		0x61, 0x73, 0x68, 0x20, 0x50, 0x6C, 0x61, 0x79,
		0x65, 0x72, 0x20, 0x30, 0x30, 0x31, /* Genuine Adobe Flash Player 001 */
		0xF0, 0xEE, 0xC2, 0x4A, 0x80, 0x68, 0xBE, 0xE8,
		0x2E, 0x00, 0xD0, 0xD1, 0x02, 0x9E, 0x7E, 0x57,
		0x6E, 0xEC, 0x5D, 0x2D, 0x29, 0x80, 0x6F, 0xAB,
		0x93, 0xB8, 0xE6, 0x36, 0xCF, 0xEB, 0x31, 0xAE,
	}
)

func GetDigestOffset1(buf []byte) (offset uint32) {
	offset = uint32(buf[8]) + uint32(buf[9]) + uint32(buf[10]) + uint32(buf[11])
	offset = offset % 728
	offset = offset + 12
	return offset
}

func GetDigestOffset2(buf []byte) (offset uint32) {
	offset = uint32(buf[772]) + uint32(buf[773]) + uint32(buf[774]) + uint32(buf[775])
	offset = offset % 728
	offset = offset + 776
	return offset
}

func GetDHOffset1(buf []byte) (offset uint32) {
	offset = uint32(buf[1532]) + uint32(buf[1533]) + uint32(buf[1534]) + uint32(buf[1535])
	offset = offset % 632
	offset = offset + 772
	return offset
}

func GetDHOffset2(buf []byte) (offset uint32) {
	offset = uint32(buf[768]) + uint32(buf[769]) + uint32(buf[770]) + uint32(buf[771])
	offset = offset % 632
	offset = offset + 8
	return offset
}

func HMACsha256(msgBytes []byte, key []byte) ([]byte, error) {
	h := hmac.New(sha256.New, key)
	_, err := h.Write(msgBytes)
	if err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func CreateRandomBlock(size uint) []byte {
	/*
		buf := make([]byte, size)
		for i := uint(0); i < size; i++ {
			buf[i] = byte(rand.Int() % 256)
		}
		return buf
	*/

	size64 := size / uint(8)
	buf := new(bytes.Buffer)
	var r64 int64
	var i uint
	for i = uint(0); i < size64; i++ {
		r64 = rand.Int63()
		binary.Write(buf, binary.BigEndian, &r64)
	}
	for i = i * uint(8); i < size; i++ {
		buf.WriteByte(byte(rand.Int()))
	}
	return buf.Bytes()

}

func Handshake(c net.Conn, br *bufio.Reader, bw *bufio.Writer, timeout time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	// Send C0+C1
	err = bw.WriteByte(0x03)
	CheckError(err, "Handshake() Send C0")
	c1 := CreateRandomBlock(RTMP_SIG_SIZE)
	// Set Timestamp
	binary.BigEndian.PutUint32(c1, uint32(GetTimestamp()))
	//	for i := 0; i < 4; i++ {
	//		c1[i] = 0
	//	}
	// Set FlashPlayer version
	for i := 0; i < 4; i++ {
		c1[4+i] = FLASH_PLAYER_VERSION[i]
	}
	// TODO: Create the DH public/private key, and use it in encryption mode
	// Get Digest offset
	clientDigestOffset := GetDigestOffset1(c1)
	// Create temp buffer
	tmpBuf := new(bytes.Buffer)
	tmpBuf.Write(c1[:clientDigestOffset])
	tmpBuf.Write(c1[clientDigestOffset+SHA256_DIGEST_LENGTH:])
	// Generate the hash
	tempHash, err := HMACsha256(tmpBuf.Bytes(), GENUINE_FP_KEY[:30])
	CheckError(err, "Handshake() Generate the C1 hash")
	for index, b := range tempHash {
		c1[clientDigestOffset+uint32(index)] = b
	}

	_, err = bw.Write(c1)
	CheckError(err, "Handshake() Send C1")
	if timeout > 0 {
		c.SetWriteDeadline(time.Now().Add(timeout))
	}
	err = bw.Flush()
	CheckError(err, "Handshake() Flush C0+C1")

	// Read S0
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	s0, err := br.ReadByte()
	CheckError(err, "Handshake() Read S0")
	if s0 != 0x03 {
		return errors.New(fmt.Sprintf("Handshake() Got S0: %x", s0))
	}

	// Read S1
	s1 := make([]byte, RTMP_SIG_SIZE)
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	_, err = io.ReadAtLeast(br, s1, RTMP_SIG_SIZE)
	CheckError(err, "Handshake Read S1")

	// Generate C2
	digestPosServer := GetDigestOffset1(s1)
	digestResp, err := HMACsha256(s1[digestPosServer:digestPosServer+SHA256_DIGEST_LENGTH], GENUINE_FP_KEY)
	CheckError(err, "Generate C2 HMACsha256 Offset1")
	c2 := CreateRandomBlock(RTMP_SIG_SIZE)
	signatureResp, err := HMACsha256(c2[:RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH], digestResp)
	CheckError(err, "Generate C2 HMACsha256 C2")
	//	DumpBuffer("signatureResp", signatureResp, 0)
	for index, b := range signatureResp {
		c2[RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH+index] = b
	}

	// Send C2
	_, err = bw.Write(c2)
	CheckError(err, "Handshake() Send C2")
	if timeout > 0 {
		c.SetWriteDeadline(time.Now().Add(timeout))
	}
	err = bw.Flush()
	CheckError(err, "Handshake() Flush C2")

	// Read S2
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	s2 := make([]byte, RTMP_SIG_SIZE)
	_, err = io.ReadAtLeast(br, s2, RTMP_SIG_SIZE)
	CheckError(err, "Handshake() Read S2")
	return
}
