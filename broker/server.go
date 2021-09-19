package broker

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	logger "github.com/bdkiran/nolan/utils"
)

type Server struct {
	host           string
	port           int
	connectionType string
	listener       net.Listener
	requestChan    chan *ReMessage
	resposeChan    chan *ReMessage
}

type ReMessage struct {
	requestType string
	body        []byte
	conn        io.ReadWriter
}

func NewServer() *Server {
	s := Server{
		host:           connHost,
		port:           connPort,
		connectionType: connType,
		requestChan:    make(chan *ReMessage, 256),
		resposeChan:    make(chan *ReMessage, 256),
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
		res := <-s.resposeChan
		s.handleResponse(res)
	}()

}

func (s *Server) handleConnection(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}

	requestMesage := string(buffer[:len(buffer)-1])

	logger.Info.Println("Client message:", string(buffer[:len(buffer)-1]))

	conn.Write(buffer)

	if requestMesage == "PRODUCER" {
		s.producerMessage(conn)
	} else if requestMesage == "CONSUMER" {
		//Consumer should pass in initial offset
		s.consumerMessage(conn, 0)
	} else {
		logger.Warning.Println("Invalid request send:", requestMesage)
		conn.Close()
	}
}

func (s *Server) producerMessage(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}
	logger.Info.Println(string(buffer[:len(buffer)-1]))
	newProduceRequest := &ReMessage{
		requestType: "PRODUCE",
		body:        buffer[:len(buffer)-1],
		conn:        conn,
	}

	s.requestChan <- newProduceRequest
}

func (s *Server) consumerMessage(conn net.Conn, offset int) {
	newProduceRequest := &ReMessage{
		requestType: "CONSUME",
		body:        []byte(strconv.Itoa(offset)),
		conn:        conn,
	}

	s.requestChan <- newProduceRequest
}

func (s *Server) handleResponse(res *ReMessage) {
	body := res.body
	res.conn.Write(body)
}
