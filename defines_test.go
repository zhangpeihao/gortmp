package gortmp

import (
	"github.com/zhangpeihao/log"
	"testing"
	"time"
)

type TestParseURLCase struct {
	name   string
	url    string
	expect RtmpURL
}

var testParseURLCase = []TestParseURLCase{
	{"full", "rtmp://somehost.com:1936/app/instance", RtmpURL{"rtmp", "somehost.com", uint16(1936), "app", "instance"}},
	{"normal", "rtmp://somehost.com/app/instance", RtmpURL{"rtmp", "somehost.com", uint16(1935), "app", "instance"}},
	{"without instance", "rtmp://somehost.com/app", RtmpURL{"rtmp", "somehost.com", uint16(1935), "app", ""}},
}

type TestParseURLErrorCase struct {
	name string
	url  string
}

var testParseURLErrorCase = []TestParseURLErrorCase{
	{"without app & instance", "rtmp://somehost.com"},
	{"no protocol", "somehost.com:1936/app/instance"},
	{"no host", "rtmp://:1936/app/instance"},
	{"no host and port", "rtmp:///app/instance"},
	{"protocol only", "rtmp://"},
	{"large port", "somehost.com:111936/app/instance"},
	{"attack1", "rtmp://rtmp://rtmp://somehost.com:111936/app/instance"},
	{"attack2", "rtmp://://://://://://somehost.com:1936/app/instance"},
	{"attack3", "rtmp:///////////somehost.com/app/instance"},
	{"attack4", "rtmp://://://://://://somehost.com/app/instance"},
}

func compareRtmpURL(a, b RtmpURL) bool {
	return (a.protocol == b.protocol) &&
		(a.host == b.host) &&
		(a.port == b.port) &&
		(a.app == b.app) &&
		(a.instanceName == b.instanceName)
}

func InitTestLogger() {
	if logger == nil {
		l := log.NewLogger(".", "test", nil, 60, 3600*24, true)
		InitLogger(l)
	}
}

func TestParseURL(t *testing.T) {
	InitTestLogger()
	for _, c := range testParseURLCase {
		got, err := ParseURL(c.url)
		if err != nil {
			t.Errorf("TestParseURL(%s - %s) error: \n%s", c.name, c.url, err.Error())
			continue
		}
		if !compareRtmpURL(got, c.expect) {
			t.Errorf("TestParseURL(%s - %s)\ngot:    %v\nexpect: %v", c.name, c.url, got, c.expect)
		}
	}
	for _, c := range testParseURLErrorCase {
		got, err := ParseURL(c.url)
		if err == nil {
			t.Errorf("TestParseURL(%s - %s) Expect error\ngot: %v", c.name, c.url, got)
		}
	}
}

func TestGetTimestamp(t *testing.T) {
	InitTestLogger()
	t1 := GetTimestamp()
	if t1 >= MAX_TIMESTAMP {
		t.Errorf("Got timestamp %d > Max value(%d)", t1, MAX_TIMESTAMP)
	}
	time.Sleep(time.Second)
	t2 := GetTimestamp()
	if t2-t1 > 1005 || t2-t1 < 995 {
		t.Errorf("Timestamp accuracy error, t2 - t1: %d", t2-t1)
	}
}
