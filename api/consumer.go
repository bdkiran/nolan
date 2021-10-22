package api

import (
	"time"

	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/utils"

	"github.com/bdkiran/nolan/logger"
)

func Consume() {
	logger.Info.Println("Poopy butthole")
}

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
			msg, err := utils.GetSocketMessage(nolanConn.socketConnection)
			if err != nil {
				logger.Error.Println(err)
				nolanConn.socketConnection.Close()
				return
			}

			srvMessageString := string(msg)
			if srvMessageString == "No Message" {
				logger.Info.Println("Server thing:", srvMessageString)
				retryMsg := utils.GetSocketBytes([]byte("RETRY"))
				nolanConn.socketConnection.Write(retryMsg)
				time.Sleep(waitTimeDuration)
			} else {
				mt, err := broker.Decode(msg)
				if err != nil {
					logger.Error.Fatalln(err)
				}
				logger.Info.Println(mt.Timestamp)
				logger.Info.Println(string(mt.Key))
				logger.Info.Println(string(mt.Value))

				awkMsg := utils.GetSocketBytes([]byte("AWK"))
				nolanConn.socketConnection.Write(awkMsg)
				timerThing.Reset(pollTimeoutDuration)
			}
		}
	}
}
