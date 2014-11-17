package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	rtmp "github.com/zhangpeihao/gortmp"
	"github.com/zhangpeihao/log"
	"io"
	"net"
	"os"
)

const (
	programName = "RtmpProxy"
	version     = "0.0.1"
)

var (
	url     *string = flag.String("URL", "rtmp://192.168.20.111/vid3", "The rtmp url to connect.")
	dumpFlv *string = flag.String("DumpFLV", "", "Dump FLV into file.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s version[%s]\r\nUsage: %s [OPTIONS]\r\n", programName, version, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	l := log.NewLogger(".", "proxy", nil, 60, 3600*24, true)
	rtmp.InitLogger(l)
	defer l.Close()

	// listen
	listen, err := net.Listen("tcp", ":1935")
	if err != nil {
		fmt.Println("Listen error", err)
		os.Exit(-1)
	}
	defer listen.Close()

	for {
		iconn, err := listen.Accept()
		if err != nil {
			fmt.Println("Accept error", err)
			os.Exit(-1)
		}
		if iconn == nil {
			fmt.Println("iconn is nil")
			os.Exit(-1)
		}
		defer iconn.Close()
		// Handshake
		// C>>>P: C0+C1
		ibr := bufio.NewReader(iconn)
		ibw := bufio.NewWriter(iconn)
		c0, err := ibr.ReadByte()
		if c0 != 0x03 {
			fmt.Printf("C>>>P: C0(0x%2x) != 0x03\n", c0)
			os.Exit(-1)
		}
		c1 := make([]byte, rtmp.RTMP_SIG_SIZE)
		_, err = io.ReadAtLeast(ibr, c1, rtmp.RTMP_SIG_SIZE)
		// Check C1
		var clientDigestOffset uint32
		if clientDigestOffset, err = CheckC1(c1, true); err != nil {
			fmt.Println("C>>>P: Test C1 err:", err)
			os.Exit(-1)
		}
		// P>>>S: Connect Server
		oconn, err := net.Dial("tcp", "192.168.20.111:1935")
		if err != nil {
			fmt.Println("P>>>S: Dial server err:", err)
			os.Exit(-1)
		}
		defer oconn.Close()
		obr := bufio.NewReader(oconn)
		obw := bufio.NewWriter(oconn)
		// P>>>S: C0+C1
		if err = obw.WriteByte(c0); err != nil {
			fmt.Println("P>>>S: Write C0 err:", err)
			os.Exit(-1)
		}
		if _, err = obw.Write(c1); err != nil {
			fmt.Println("P>>>S: Write C1 err:", err)
			os.Exit(-1)
		}
		if err = obw.Flush(); err != nil {
			fmt.Println("P>>>S: Flush err:", err)
			os.Exit(-1)
		}
		// P<<<S: Read S0+S1+S2
		s0, err := obr.ReadByte()
		if err != nil {
			fmt.Println("P<<<S: Read S0 err:", err)
			os.Exit(-1)
		}
		if c0 != 0x03 {
			fmt.Printf("P<<<S: S0(0x%2x) != 0x03\n", s0)
			os.Exit(-1)
		}
		s1 := make([]byte, rtmp.RTMP_SIG_SIZE)
		_, err = io.ReadAtLeast(obr, s1, rtmp.RTMP_SIG_SIZE)
		if err != nil {
			fmt.Println("P<<<S: Read S1 err:", err)
			os.Exit(-1)
		}
		s2 := make([]byte, rtmp.RTMP_SIG_SIZE)
		_, err = io.ReadAtLeast(obr, s2, rtmp.RTMP_SIG_SIZE)
		if err != nil {
			fmt.Println("P<<<S: Read S2 err:", err)
			os.Exit(-1)
		}

		// C<<<P: Send S0+S1+S2
		if err = ibw.WriteByte(s0); err != nil {
			fmt.Println("C<<<P: Write S0 err:", err)
			os.Exit(-1)
		}
		if _, err = ibw.Write(s1); err != nil {
			fmt.Println("C<<<P: Write S1 err:", err)
			os.Exit(-1)
		}
		if _, err = ibw.Write(s2); err != nil {
			fmt.Println("C<<<P: Write S2 err:", err)
			os.Exit(-1)
		}
		if err = ibw.Flush(); err != nil {
			fmt.Println("C<<<P: Flush err:", err)
			os.Exit(-1)
		}

		// C>>>P: Read C2
		c2 := make([]byte, rtmp.RTMP_SIG_SIZE)
		_, err = io.ReadAtLeast(ibr, c2, rtmp.RTMP_SIG_SIZE)

		// Check S2
		server_pos := rtmp.ValidateDigest(s1, 8, rtmp.GENUINE_FP_KEY[:30])
		if server_pos == 0 {
			server_pos = rtmp.ValidateDigest(s1, 772, rtmp.GENUINE_FP_KEY[:30])
			if server_pos == 0 {
				fmt.Println("P<<<S: S1 position check error")
				os.Exit(-1)
			}
		}

		digest, err := rtmp.HMACsha256(c1[clientDigestOffset:clientDigestOffset+rtmp.SHA256_DIGEST_LENGTH], rtmp.GENUINE_FMS_KEY)
		rtmp.CheckError(err, "Get digest from c1 error")

		signature, err := rtmp.HMACsha256(s2[:rtmp.RTMP_SIG_SIZE-rtmp.SHA256_DIGEST_LENGTH], digest)
		rtmp.CheckError(err, "Get signature from s2 error")

		if bytes.Compare(signature, s2[rtmp.RTMP_SIG_SIZE-rtmp.SHA256_DIGEST_LENGTH:]) != 0 {
			fmt.Println("Server signature mismatch")
			os.Exit(-1)
		}

		digestResp, err := rtmp.HMACsha256(s1[server_pos:server_pos+rtmp.SHA256_DIGEST_LENGTH], rtmp.GENUINE_FP_KEY)
		rtmp.CheckError(err, "Generate C2 HMACsha256 digestResp")
		signatureResp, err := rtmp.HMACsha256(c2[:rtmp.RTMP_SIG_SIZE-rtmp.SHA256_DIGEST_LENGTH], digestResp)
		if bytes.Compare(signatureResp, c2[rtmp.RTMP_SIG_SIZE-rtmp.SHA256_DIGEST_LENGTH:]) != 0 {
			fmt.Println("C2 mismatch")
			os.Exit(-1)
		}

		// P>>>S: Send C2
		if _, err = obw.Write(c2); err != nil {
			fmt.Println("P>>>S: Write C2 err:", err)
			os.Exit(-1)
		}
		if err = obw.Flush(); err != nil {
			fmt.Println("P>>>S: Flush err:", err)
			os.Exit(-1)
		}

		// Proxy
		go io.Copy(iconn, oconn)
		go io.Copy(oconn, iconn)
	}
}

func CheckC1(c1 []byte, offset1 bool) (uint32, error) {
	var clientDigestOffset uint32
	if offset1 {
		clientDigestOffset = rtmp.CalcDigestPos(c1, 8, 728, 12)
	} else {
		clientDigestOffset = rtmp.CalcDigestPos(c1, 772, 728, 776)
	}
	// Create temp buffer
	tmpBuf := new(bytes.Buffer)
	tmpBuf.Write(c1[:clientDigestOffset])
	tmpBuf.Write(c1[clientDigestOffset+rtmp.SHA256_DIGEST_LENGTH:])
	// Generate the hash
	tempHash, err := rtmp.HMACsha256(tmpBuf.Bytes(), rtmp.GENUINE_FP_KEY[:30])
	if err != nil {
		return 0, errors.New(fmt.Sprintf("HMACsha256 err: %s\n", err.Error()))
	}
	expect := c1[clientDigestOffset : clientDigestOffset+rtmp.SHA256_DIGEST_LENGTH]
	if bytes.Compare(expect, tempHash) != 0 {
		return 0, errors.New(fmt.Sprintf("C1\nExpect % 2x\nGot    % 2x\n",
			expect,
			tempHash))
	}
	return clientDigestOffset, nil
}

func CheckC2(s1, c2 []byte) (uint32, error) {
	server_pos := rtmp.ValidateDigest(s1, 8, rtmp.GENUINE_FMS_KEY[:36])
	if server_pos == 0 {
		server_pos = rtmp.ValidateDigest(s1, 772, rtmp.GENUINE_FMS_KEY[:36])
		if server_pos == 0 {
			return 0, errors.New("Server response validating failed")
		}
	}

	digest, err := rtmp.HMACsha256(s1[server_pos:server_pos+rtmp.SHA256_DIGEST_LENGTH], rtmp.GENUINE_FP_KEY)
	rtmp.CheckError(err, "Get digest from s1 error")

	signature, err := rtmp.HMACsha256(c2[:rtmp.RTMP_SIG_SIZE-rtmp.SHA256_DIGEST_LENGTH], digest)
	rtmp.CheckError(err, "Get signature from c2 error")

	if bytes.Compare(signature, c2[rtmp.RTMP_SIG_SIZE-rtmp.SHA256_DIGEST_LENGTH:]) != 0 {
		return 0, errors.New("Server signature mismatch")
	}
	return server_pos, nil
}
