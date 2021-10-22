package utils

import (
	"encoding/binary"
	"net"

	"github.com/bdkiran/nolan/logger"
)

func GetSocketMessage(conn net.Conn) ([]byte, error) {
	size, err := getSocketMessageSize(conn)
	if err != nil {
		logger.Error.Println(err)
		return []byte{}, err
	}
	reply := make([]byte, size)
	if _, err := conn.Read(reply); err != nil {
		return []byte{}, err
	}
	return reply, nil
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

func GetSocketBytes(msg []byte) []byte {
	fullMsg := make([]byte, 4)
	binary.LittleEndian.PutUint32(fullMsg, uint32(len(msg)))
	fullMsg = append(fullMsg, msg...)
	return fullMsg
}
