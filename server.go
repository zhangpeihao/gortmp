// Copyright 2013, zhangpeihao All rights reserved.
package gortmp

import (
	"bufio"
	"github.com/zhangpeihao/log"
	"net"
	"time"
)

type ServerHandler interface {
	NewConnection(conn InboundConn, connectReq *Command, server *Server) bool
}

type Server struct {
	listener    net.Listener
	network     string
	bindAddress string
	exit        bool
	handler     ServerHandler
}

// Create a new server.
func NewServer(network string, bindAddress string, handler ServerHandler) (*Server, error) {
	server := &Server{
		network:     network,
		bindAddress: bindAddress,
		exit:        false,
		handler:     handler,
	}
	var err error
	server.listener, err = net.Listen(server.network, server.bindAddress)
	if err != nil {
		return nil, err
	}
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG,
		"Start listen...")
	go server.mainLoop()
	return server, nil
}

// Close listener.
func (server *Server) Close() {
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_TRACE,
		"Stop server")
	server.exit = true
	server.listener.Close()
}

func (server *Server) mainLoop() {
	for !server.exit {
		c, err := server.listener.Accept()
		if err != nil {
			if server.exit {
				break
			}
			logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
				"SocketServer listener error:", err)
			server.rebind()
		}
		if c != nil {
			go server.Handshake(c)
		}
	}
}

func (server *Server) rebind() {
	listener, err := net.Listen(server.network, server.bindAddress)
	if err == nil {
		server.listener = listener
	} else {
		time.Sleep(time.Second)
	}
}

func (server *Server) Handshake(c net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
				"Server::Handshake panic error:", err)
		}
	}()
	logger.ModulePrintln(logHandler, log.LOG_LEVEL_DEBUG,
		"Handshake begin")
	br := bufio.NewReader(c)
	bw := bufio.NewWriter(c)
	timeout := time.Duration(10) * time.Second
	if err := SHandshake(c, br, bw, timeout); err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"SHandshake error:", err)
		c.Close()
		return
	}
	// New inbound connection
	_, err := NewInboundConn(c, br, bw, server, 100)
	if err != nil {
		logger.ModulePrintln(logHandler, log.LOG_LEVEL_WARNING,
			"NewInboundConn error:", err)
		c.Close()
		return
	}
}

// On received connect request
func (server *Server) OnConnectAuth(conn InboundConn, connectReq *Command) bool {
	return server.handler.NewConnection(conn, connectReq, server)
}
