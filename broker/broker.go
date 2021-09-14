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
	connHost = "127.0.0.1"
	connPort = "6969"
	connType = "tcp"
)

var COMMITLOG *commitlog.Commitlog

func Main() {
	var err error
	COMMITLOG, err = commitlog.New("logs/partition0")
	if err != nil {
		logger.Error.Fatalln("Unable to initilizw commitlog", err)
	}

	fmt.Println("Starting " + connType + " server on " + connHost + ":" + connPort)
	l, err := net.Listen(connType, connHost+":"+connPort)
	if err != nil {
		logger.Error.Fatalln("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
			return
		}
		logger.Info.Println("Client " + c.RemoteAddr().String() + " connected.")

		go handleConnection(c)
	}
}

func handleConnection(conn net.Conn) {
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
		producer(conn)
	} else if requestMesage == "CONSUMER" {
		consumer(conn)
	} else {
		logger.Warning.Println("Invalid request send:", requestMesage)
		conn.Close()
	}
	//handleConnection(conn)
}

func producer(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}

	requestMesage := string(buffer[:len(buffer)-1])

	logger.Info.Println("Client message:", requestMesage)

	COMMITLOG.Append([]byte(requestMesage))

	conn.Write(buffer)
	producer(conn)
}

func consumer(conn net.Conn) {
	buffer, err := bufio.NewReader(conn).ReadBytes('\n')

	if err != nil {
		logger.Warning.Println("Client left.")
		conn.Close()
		return
	}

	COMMITLOG.ReadLatestEntry()
	requestMesage := string(buffer[:len(buffer)-1])

	logger.Info.Println("Client message:", requestMesage)

	conn.Write(buffer)
	producer(conn)
}
