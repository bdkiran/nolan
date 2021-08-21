package utils

import (
	"io"
	"log"
	"os"
)

//Logger is the logger structure that will provide logging fetures for the project
var (
	//Info level logging
	Info *log.Logger
	//Warning level logging
	Warning *log.Logger
	//Error level logging
	Error *log.Logger
)

//LoggerInit initializes the gloabal logger to be used across the application
func LoggerInit(writeToFile bool) {
	var multi io.Writer
	if writeToFile {
		fileName := "app.log"
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to open log file : " + fileName)
		}
		multi = io.MultiWriter(file, os.Stdout)
	} else {
		multi = io.MultiWriter(os.Stdout)
	}
	Info = log.New(multi,
		"INFO: ",
		log.Ldate|log.Ltime|log.Llongfile)

	Warning = log.New(multi,
		"WARNING: ",
		log.Ldate|log.Ltime|log.Llongfile)

	Error = log.New(multi,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Llongfile)
}
