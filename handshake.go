// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

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

	"github.com/zhangpeihao/log"
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

func CalcDigestPos(buf []byte, offset uint32, mod_val uint32, add_val uint32) (digest_pos uint32) {
	var i uint32
	for i = 0; i < 4; i++ {
		digest_pos += uint32(buf[i+offset])
	}
	digest_pos = digest_pos%mod_val + add_val
	return
}

func CalcDHPos(buf []byte, offset uint32, mod_val uint32, add_val uint32) (digest_pos uint32) {
	var i uint32
	for i = 0; i < 4; i++ {
		digest_pos += uint32(buf[i+offset])
	}
	digest_pos = digest_pos%mod_val + add_val
	return
}

func ValidateDigest(buf []byte, offset uint32, key []byte) uint32 {
	digestPos := CalcDigestPos(buf, offset, 728, offset+4)
	// Create temp buffer
	tmpBuf := new(bytes.Buffer)
	tmpBuf.Write(buf[:digestPos])
	tmpBuf.Write(buf[digestPos+SHA256_DIGEST_LENGTH:])
	// Generate the hash
	tempHash, err := HMACsha256(tmpBuf.Bytes(), key)
	if err != nil {
		return 0
	}
	if bytes.Compare(tempHash, buf[digestPos:digestPos+SHA256_DIGEST_LENGTH]) == 0 {
		return digestPos
	}
	return 0
}

func ImprintWithDigest(buf []byte, key []byte) uint32 {
	//digestPos := CalcDigestPos(buf, 772, 728, 776)
	digestPos := CalcDigestPos(buf, 8, 728, 12)

	// Create temp buffer
	tmpBuf := new(bytes.Buffer)
	tmpBuf.Write(buf[:digestPos])
	tmpBuf.Write(buf[digestPos+SHA256_DIGEST_LENGTH:])
	// Generate the hash
	tempHash, err := HMACsha256(tmpBuf.Bytes(), key)
	if err != nil {
		return 0
	}
	for index, b := range tempHash {
		buf[digestPos+uint32(index)] = b
	}
	return digestPos
}

func HandshakeSample(c net.Conn, br *bufio.Reader, bw *bufio.Writer, timeout time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	// Send C0+C1
	err = bw.WriteByte(0x03)
	c1 := CreateRandomBlock(RTMP_SIG_SIZE)
	for i := 0; i < 8; i++ {
		c1[i] = 0
	}
	bw.Write(c1)
	err = bw.Flush()
	CheckError(err, "Handshake() Flush C0+C1")
	// Read S0+S1+S2
	s0, err := br.ReadByte()
	CheckError(err, "Handshake() Read S0")
	if s0 != 0x03 {
		return errors.New(fmt.Sprintf("Handshake() Got S0: %x", s0))
	}
	s1 := make([]byte, RTMP_SIG_SIZE)
	_, err = io.ReadAtLeast(br, s1, RTMP_SIG_SIZE)
	CheckError(err, "Handshake() Read S1")
	bw.Write(s1)
	err = bw.Flush()
	CheckError(err, "Handshake() Flush C2")
	_, err = io.ReadAtLeast(br, s1, RTMP_SIG_SIZE)
	CheckError(err, "Handshake() Read S2")
	return
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
	// binary.BigEndian.PutUint32(c1, uint32(GetTimestamp()))
	binary.BigEndian.PutUint32(c1, uint32(0))
	// Set FlashPlayer version
	for i := 0; i < 4; i++ {
		c1[4+i] = FLASH_PLAYER_VERSION[i]
	}

	// TODO: Create the DH public/private key, and use it in encryption mode

	clientDigestOffset := ImprintWithDigest(c1, GENUINE_FP_KEY[:30])
	if clientDigestOffset == 0 {
		return errors.New("ImprintWithDigest failed")
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
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
		"Handshake() FMS version is %d.%d.%d.%d", s1[4], s1[5], s1[6], s1[7])
	//	if s1[4] < 3 {
	//		return errors.New(fmt.Sprintf("FMS version is %d.%d.%d.%d, unsupported!", s1[4], s1[5], s1[6], s1[7]))
	//	}

	// Read S2
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	s2 := make([]byte, RTMP_SIG_SIZE)
	_, err = io.ReadAtLeast(br, s2, RTMP_SIG_SIZE)
	CheckError(err, "Handshake() Read S2")

	// Check server response
	server_pos := ValidateDigest(s1, 8, GENUINE_FMS_KEY[:36])
	if server_pos == 0 {
		server_pos = ValidateDigest(s1, 772, GENUINE_FMS_KEY[:36])
		if server_pos == 0 {
			return errors.New("Server response validating failed")
		}
	}

	digest, err := HMACsha256(c1[clientDigestOffset:clientDigestOffset+SHA256_DIGEST_LENGTH], GENUINE_FMS_KEY)
	CheckError(err, "Get digest from c1 error")

	signature, err := HMACsha256(s2[:RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH], digest)
	CheckError(err, "Get signature from s2 error")

	if bytes.Compare(signature, s2[RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH:]) != 0 {
		return errors.New("Server signature mismatch")
	}

	// Generate C2
	// server_pos := GetDigestOffset1(s1)
	digestResp, err := HMACsha256(s1[server_pos:server_pos+SHA256_DIGEST_LENGTH], GENUINE_FP_KEY)
	CheckError(err, "Generate C2 HMACsha256 digestResp")

	c2 := CreateRandomBlock(RTMP_SIG_SIZE)
	signatureResp, err := HMACsha256(c2[:RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH], digestResp)
	CheckError(err, "Generate C2 HMACsha256 signatureResp")
	DumpBuffer("signatureResp", signatureResp, 0)
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

	if timeout > 0 {
		c.SetDeadline(time.Time{})
	}

	return
}

func SHandshake(c net.Conn, br *bufio.Reader, bw *bufio.Writer, timeout time.Duration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	// Send S0+S1
	err = bw.WriteByte(0x03)
	CheckError(err, "SHandshake() Send S0")
	s1 := CreateRandomBlock(RTMP_SIG_SIZE)
	// Set Timestamp
	// binary.BigEndian.PutUint32(s1, uint32(GetTimestamp()))
	binary.BigEndian.PutUint32(s1, uint32(0))
	// Set FlashPlayer version
	for i := 0; i < 4; i++ {
		s1[4+i] = FMS_VERSION[i]
	}

	serverDigestOffset := ImprintWithDigest(s1, GENUINE_FMS_KEY[:36])
	if serverDigestOffset == 0 {
		return errors.New("ImprintWithDigest failed")
	}

	_, err = bw.Write(s1)
	CheckError(err, "SHandshake() Send S1")
	if timeout > 0 {
		c.SetWriteDeadline(time.Now().Add(timeout))
	}
	err = bw.Flush()
	CheckError(err, "SHandshake() Flush S0+S1")

	// Read C0
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	c0, err := br.ReadByte()
	CheckError(err, "SHandshake() Read C0")
	if c0 != 0x03 {
		return errors.New(fmt.Sprintf("SHandshake() Got C0: %x", c0))
	}

	// Read C1
	c1 := make([]byte, RTMP_SIG_SIZE)
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	_, err = io.ReadAtLeast(br, c1, RTMP_SIG_SIZE)
	CheckError(err, "SHandshake Read C1")
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
		"SHandshake() Flash player version is %d.%d.%d.%d", c1[4], c1[5], c1[6], c1[7])

	scheme := 0
	clientDigestOffset := ValidateDigest(c1, 8, GENUINE_FP_KEY[:30])
	if clientDigestOffset == 0 {
		clientDigestOffset = ValidateDigest(c1, 772, GENUINE_FP_KEY[:30])
		if clientDigestOffset == 0 {
			return errors.New("SHandshake C1 validating failed")
		}
		scheme = 1
	}
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
		"SHandshake() scheme = %d", scheme)
	digestResp, err := HMACsha256(c1[clientDigestOffset:clientDigestOffset+SHA256_DIGEST_LENGTH], GENUINE_FMS_KEY)
	CheckError(err, "SHandshake Generate digestResp")

	// Generate S2
	s2 := CreateRandomBlock(RTMP_SIG_SIZE)
	signatureResp, err := HMACsha256(s2[:RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH], digestResp)
	CheckError(err, "SHandshake Generate S2 HMACsha256 signatureResp")
	DumpBuffer("SHandshake signatureResp", signatureResp, 0)
	for index, b := range signatureResp {
		s2[RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH+index] = b
	}

	// Send S2
	_, err = bw.Write(s2)
	CheckError(err, "SHandshake() Send S2")

	if timeout > 0 {
		c.SetWriteDeadline(time.Now().Add(timeout))
	}
	err = bw.Flush()
	CheckError(err, "SHandshake() Flush S2")

	// Read C2
	if timeout > 0 {
		c.SetReadDeadline(time.Now().Add(timeout))
	}
	c2 := make([]byte, RTMP_SIG_SIZE)
	_, err = io.ReadAtLeast(br, c2, RTMP_SIG_SIZE)
	CheckError(err, "SHandshake() Read C2")
	// TODO: check C2
	if timeout > 0 {
		c.SetDeadline(time.Time{})
	}
	return
}
