package rtmp

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

const (
	TEST_FMS_URL = "rtmp://192.168.20.111/live"
)

func testC1(c1 []byte, offset1 bool) error {
	var clientDigestOffset uint32
	if offset1 {
		clientDigestOffset = CalcDigestPos(c1, 8, 728, 12)
	} else {
		clientDigestOffset = CalcDigestPos(c1, 772, 728, 776)
	}
	// Create temp buffer
	tmpBuf := new(bytes.Buffer)
	tmpBuf.Write(c1[:clientDigestOffset])
	tmpBuf.Write(c1[clientDigestOffset+SHA256_DIGEST_LENGTH:])
	// Generate the hash
	tempHash, err := HMACsha256(tmpBuf.Bytes(), GENUINE_FP_KEY[:30])
	if err != nil {
		return errors.New(fmt.Sprintf("HMACsha256 err: %s\n", err.Error()))
	}
	expect := c1[clientDigestOffset : clientDigestOffset+SHA256_DIGEST_LENGTH]
	if bytes.Compare(expect, tempHash) != 0 {
		return errors.New(fmt.Sprintf("C1\nExpect % 2x\nGot    % 2x\n",
			expect,
			tempHash))
	}
	return nil
}

var (
	testCases = [][]byte{
		vlc_c1, vlc_c2, vlc_s1,
		my_c1, my_c2, my_s1,
		fp_c1, fp_c2, fp_s1,
	}
)

func TestHandshake(t *testing.T) {
	caseNum := len(testCases) / 3
	for i := 0; i < caseNum; i++ {
		c1 := testCases[i*3]
		c2 := testCases[i*3+1]
		s1 := testCases[i*3+2]

		if err := testC1(c1, true); err != nil {
			if err = testC1(c1, false); err != nil {
				t.Error(err.Error())
			}
		}

		digestPosServer := CalcDigestPos(s1, 8, 728, 12)
		digestResp, err := HMACsha256(s1[digestPosServer:digestPosServer+SHA256_DIGEST_LENGTH], GENUINE_FP_KEY)
		if err != nil {
			t.Fatalf("Generate C2 HMACsha256 Offset1 err: %s\n", err.Error())
		}
		signatureResp, err := HMACsha256(c2[:RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH], digestResp)
		if err != nil {
			t.Fatalf("Generate C2 HMACsha256 C2 err: %s\n", err.Error())
		}
		expect := c2[RTMP_SIG_SIZE-SHA256_DIGEST_LENGTH:]
		if bytes.Compare(expect, signatureResp) != 0 {
			t.Fatalf("C2\nExpect % 2x\nGot    % 2x\n",
				expect,
				signatureResp)
		}
	}
}
