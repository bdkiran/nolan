package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/bdkiran/nolan/broker"
	"github.com/bdkiran/nolan/commitlog"
	"github.com/bdkiran/nolan/logger"
)

func main() {
	logger.LoggerInit(false)

	mockClient := flag.String("client", "", "Flag to start the test producer or consumer")
	debugPtr := flag.Bool("debug", false, "Debug the commitlog by reading it")

	flag.Parse()

	if *debugPtr {
		Debug()
		return
	}

	logger.Info.Println(*mockClient)
	if *mockClient == "producer" {
		ProduceMessages()
		return
	} else if *mockClient == "consumer" {
		ConsumeMessages()
		return
	} else {
		printArt()
		RunBroker()
	}

}

func RunBroker() {
	finish := make(chan bool)
	broker := broker.NewBroker()
	if len(broker.GetTopics()) == 0 {
		broker.CreateTopic("topic1", "logs/partition0")
	}
	go broker.Server.StartServer()
	go broker.Run()
	<-finish
}

func Debug() {
	logger.LoggerInit(false)
	logger.Info.Println("Nolan Starting up...")
	cl, _ := commitlog.New("logs/partition0")
	// cl.Append([]byte("We made it"))
	// cl.Append([]byte("Dont hate it"))
	// cl.Append([]byte("Another test"))
	//cl.ReadLatestEntry()
	//cl.ReadAll()
	i := 0
	for {
		logger.Info.Println("Looking for offset: ", i)
		buf, err := cl.Read(i)
		if err != nil {
			logger.Error.Println(err)
			return
		}
		logger.Info.Println(string(buf))
		i++
	}
}

func DebugEncoder() {
	tthing := time.Now()
	logger.Info.Println(tthing)
	m := broker.Message{
		Timestamp: tthing,
		Key:       []byte("hello"),
		Value:     []byte("world"),
	}
	x, _ := m.Encode()
	logger.Info.Print(x)
	mt, err := broker.Decode(x)
	if err != nil {
		logger.Error.Fatalln(err)
	}
	logger.Info.Println(mt.Timestamp)
	logger.Info.Println(string(mt.Key))
	logger.Info.Println(string(mt.Value))
}

func printArt() {
	asciiArt :=
		`
=============================================================================
_____  ___     ______   ___           __     _____  ___   
(\"   \|"  \   /    " \ |"  |         /""\   (\"   \|"  \  
|.\\   \    | // ____  \||  |        /    \  |.\\   \    | 
|: \.   \\  |/  /    ) :):  |       /' /\  \ |: \.   \\  | 
|.  \    \. (: (____/ // \  |___   //  __'  \|.  \    \. | 
|    \    \ |\        / ( \_|:  \ /   /  \\  \    \    \ | 
 \___|\____\) \"_____/   \_______|___/    \___)___|\____\) 
                                                           																		  
=============================================================================
`
	fmt.Println(asciiArt)
}
