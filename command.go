// Copyright 2013, zhangpeihao All rights reserved.

package gortmp

import (
	"github.com/zhangpeihao/goamf"
	"github.com/zhangpeihao/log"
)

// Command
//
// Command messages carry the AMF encoded commands between the client
// and the server. A client or a server can request Remote Procedure
// Calls (RPC) over streams that are communicated using the command
// messages to the peer.
type Command struct {
	IsFlex        bool
	Name          string
	TransactionID uint32
	Objects       []interface{}
}

func (cmd *Command) Write(w Writer) (err error) {
	if cmd.IsFlex {
		err = w.WriteByte(0x00)
		if err != nil {
			return
		}
	}
	_, err = amf.WriteString(w, cmd.Name)
	if err != nil {
		return
	}
	_, err = amf.WriteDouble(w, float64(cmd.TransactionID))
	if err != nil {
		return
	}
	for _, object := range cmd.Objects {
		_, err = amf.WriteValue(w, object)
		if err != nil {
			return
		}
	}
	return
}

func (cmd *Command) Dump() {
	logger.ModulePrintf(logHandler, log.LOG_LEVEL_DEBUG,
		"Command{IsFlex: %t, Name: %s, TransactionID: %d, Objects: %+v}\n",
		cmd.IsFlex, cmd.Name, cmd.TransactionID, cmd.Objects)
}
