package broker

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/bdkiran/nolan/commitlog"
	logger "github.com/bdkiran/nolan/utils"
)

const (
	connHost    = "127.0.0.1"
	connPort    = 6969
	connType    = "tcp"
	clDirectory = "logs/partition0"
)

type Server struct {
	host           string
	port           int
	connectionType string
	listener       net.Listener
	commitlog      *commitlog.Commitlog
}

func NewServer() *Server {
	s := Server{
		host:           connHost,
		port:           connPort,
		connectionType: connType,
	}

	//Create a commitlog for our server
	cl, err := commitlog.New("logs/partition0")
	if err != nil {
		logger.Error.Fatalln("Unable to initilize commitlog", err)
	}

	s.commitlog = cl
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
	defer listen.Close()

	s.listener = listen

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
			return
		}
		logger.Info.Println("Client " + conn.RemoteAddr().String() + " connected.")

		go s.handleConnection(conn)
	}

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
		s.producer(conn)
	} else if requestMesage == "CONSUMER" {
		//Consumer should pass in initial offset
		s.consumer(conn, 0)
	} else {
		logger.Warning.Println("Invalid request send:", requestMesage)
		conn.Close()
	}
}

func (s *Server) producer(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}

	requestMesage := string(buffer[:len(buffer)-1])

	s.commitlog.Append([]byte(requestMesage))
	conn.Write([]byte("AWK\n"))
	s.producer(conn)
}

func (s *Server) consumer(conn net.Conn, offset int) {
	requestMesageBuffer, err := s.commitlog.Read(offset)
	if err != nil {
		logger.Warning.Println("Closing connection.", err)
		conn.Close()
		return
	}
	requestMesageBuffer = append(requestMesageBuffer, "\n"...)
	conn.Write(requestMesageBuffer)

	offset++

	//Verify that we get to AWK message...
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}

	s.consumer(conn, offset)

}
