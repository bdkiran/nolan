package broker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	logger "github.com/bdkiran/nolan/utils"
)

// These should not be hard coded, pass these in as a configuration
const (
	connHost = "127.0.0.1"
	connPort = 6969
	connType = "tcp"
)

type Server struct {
	host           string
	port           int
	connectionType string
	listener       net.Listener
	requestChan    chan *SocketMessage
	resposeChan    chan *SocketMessage
}

type socketMessageType string

const (
	PRODUCER socketMessageType = "PRODUCER"
	CONSUMER socketMessageType = "CONSUMER"
)

type SocketMessage struct {
	MessageType socketMessageType
	topic       string
	body        []byte
	conn        io.ReadWriter
}

func NewServer() *Server {
	s := Server{
		host:           connHost,
		port:           connPort,
		connectionType: connType,
		requestChan:    make(chan *SocketMessage, 256),
		resposeChan:    make(chan *SocketMessage, 256),
	}

	return &s
}

func (s *Server) StartServer() {
	//Start up listening for the server
	logger.Info.Printf("Starting %s server on %d\n", s.host, s.port)

	connectionString := fmt.Sprintf("%s:%d", s.host, s.port)
	listen, err := net.Listen(connType, connectionString)
	if err != nil {
		logger.Error.Fatalln("Error listening:", err.Error())
		os.Exit(1)
	}
	//defer listen.Close()
	s.listener = listen

	go func() {
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				fmt.Println("Error connecting:", err.Error())
				return
			}
			logger.Info.Println("Client " + conn.RemoteAddr().String() + " connected.")

			go s.handleConnection(conn)
		}
	}()

	go func() {
		for {
			res := <-s.resposeChan
			s.handleResponse(res)
		}
	}()
}

func (s *Server) handleConnection(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}
	requestMessage, topic, _ := parseConnectionMessage(buffer)

	logger.Info.Println("Client message:", string(buffer[:len(buffer)-1]))

	conn.Write(buffer)

	if requestMessage == "PRODUCER" {
		s.producerMessage(conn, topic)
	} else if requestMessage == "CONSUMER" {
		//Consumer should pass in initial offset
		s.consumerMessage(conn, 0, topic)
	} else {
		logger.Warning.Println("Invalid request sent:", requestMessage)
		conn.Close()
	}
}

func parseConnectionMessage(connectionMessage []byte) (string, string, error) {
	splitConMsg := connectionMessage[:len(connectionMessage)-1]
	newBytes := bytes.Split(splitConMsg, []byte(":"))
	logger.Info.Println(string(newBytes[0]))
	logger.Info.Println(string(newBytes[1]))

	return string(newBytes[0]), string(newBytes[1]), nil

}

func (s *Server) producerMessage(conn net.Conn, topic string) {
	size, err := getSocketMessageSize(conn)
	if err != nil {
		logger.Error.Println(err)
		conn.Close()
		return
	}
	b := make([]byte, size)
	if _, err = conn.Read(b); err != nil {
		logger.Error.Println("Client left. Unable to read message.", err)
		conn.Close()
		return
	}

	newProduceRequest := &SocketMessage{
		MessageType: PRODUCER,
		topic:       topic,
		body:        b,
		conn:        conn,
	}

	s.requestChan <- newProduceRequest
	s.producerMessage(conn, topic)
}

func (s *Server) consumerMessage(conn net.Conn, offset int, topic string) {
	newConsumeRequest := &SocketMessage{
		MessageType: CONSUMER,
		topic:       topic,
		body:        []byte(strconv.Itoa(offset)),
		conn:        conn,
	}
	s.requestChan <- newConsumeRequest

	//Verify that we get to AWK message...
	size, err := getSocketMessageSize(conn)
	if err != nil {
		logger.Error.Println(err)
		conn.Close()
		return
	}
	b := make([]byte, size)
	if _, err = conn.Read(b); err != nil {
		logger.Error.Println("Client left. Unable to read message.", err)
		conn.Close()
		return
	}

	if string(b) != "RETRY" {
		logger.Info.Print(string(b))
		offset++
	}
	s.consumerMessage(conn, offset, topic)
}

func (s *Server) handleResponse(res *SocketMessage) {
	body := res.body
	res.conn.Write(body)
}

func getSocketMessageSize(conn net.Conn) (uint32, error) {
	p := make([]byte, 4)
	_, err := conn.Read(p)
	if err != nil {
		return uint32(0), err
	}
	size := binary.LittleEndian.Uint32(p)
	return size, nil
}
