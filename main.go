package main

import (
	"log"

	"github.com/bdkiran/nolan/commitlog"
)

func main() {
	log.Println("Started the program")
	cl, _ := commitlog.New("logs/partition")
	cl.Append([]byte("We made it"))
	cl.Append([]byte("Dont hate it"))
	cl.Append([]byte("Another test"))
	cl.ReadAll()
}
