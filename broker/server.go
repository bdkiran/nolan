package broker

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/bdkiran/nolan/logger"
	"github.com/bdkiran/nolan/utils"
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
	msg, err := utils.GetSocketMessage(conn)
	if err != nil {
		logger.Error.Println(err)
		conn.Close()
		return
	}
	logger.Info.Println("Client message:", string(msg))
	requestMessage, topic := parseConnectionMessage(msg)

	//Send AWK message
	socketMsg := utils.GetSocketBytes([]byte("AWK"))
	conn.Write(socketMsg)

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

func parseConnectionMessage(connectionMessage []byte) (string, string) {
	splitBytes := bytes.Split(connectionMessage, []byte(":"))
	logger.Info.Println(string(splitBytes[0]))
	logger.Info.Println(string(splitBytes[1]))

	return string(splitBytes[0]), string(splitBytes[1])

}

func (s *Server) producerMessage(conn net.Conn, topic string) {
	msg, err := utils.GetSocketMessage(conn)
	if err != nil {
		logger.Error.Println(err)
		conn.Close()
		return
	}

	newProduceRequest := &SocketMessage{
		MessageType: PRODUCER,
		topic:       topic,
		body:        msg,
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

	msg, err := utils.GetSocketMessage(conn)
	if err != nil {
		logger.Error.Println(err)
		conn.Close()
		return
	}

	switch returnMsg := string(msg); returnMsg {
	case "AWK":
		offset++
		s.consumerMessage(conn, offset, topic)
	case "RETRY":
		s.consumerMessage(conn, offset, topic)
	default:
		logger.Error.Println("Did not recieved expected response: ", returnMsg)
		conn.Close()
	}
}

func (s *Server) handleResponse(res *SocketMessage) {
	body := res.body
	res.conn.Write(body)
}
