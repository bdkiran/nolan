package api

import (
	"time"

	"github.com/bdkiran/nolan/broker"
	logger "github.com/bdkiran/nolan/utils"
)

func (nolanConn *nolanConnection) ConsumeMessages(pollTimeout int, waitTime int) {
	pollTimeoutDuration := time.Duration(pollTimeout) * time.Second
	waitTimeDuration := time.Duration(waitTime) * time.Second

	timerThing := time.NewTimer(pollTimeoutDuration)

	var i int
	for {
		i++
		select {
		case <-timerThing.C:
			logger.Warning.Println("Consumer timeout hit. Closing Connection.")
			nolanConn.socketConnection.Close()
			return
		default:
			size, err := getSocketMessageSize(nolanConn.socketConnection)
			if err != nil {
				logger.Error.Println(err)
				nolanConn.socketConnection.Close()
				return
			}
			b := make([]byte, size)
			if _, err = nolanConn.socketConnection.Read(b); err != nil {
				logger.Error.Println("Client left. Unable to read message.", err)
				nolanConn.socketConnection.Close()
				return
			}

			srvMessageString := string(b)
			if srvMessageString == "No Message" {
				logger.Info.Println("Server thing:", srvMessageString)
				retryMsg := getSocketBytes([]byte("RETRY"))
				nolanConn.socketConnection.Write(retryMsg)
				time.Sleep(waitTimeDuration)
			} else {
				mt, err := broker.Decode(b)
				if err != nil {
					logger.Error.Fatalln(err)
				}
				logger.Info.Println(mt.Timestamp)
				logger.Info.Println(string(mt.Key))
				logger.Info.Println(string(mt.Value))

				awkMsg := getSocketBytes([]byte("AWK"))
				nolanConn.socketConnection.Write(awkMsg)
				timerThing.Reset(pollTimeoutDuration)
			}
		}
	}
}
