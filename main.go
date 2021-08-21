package main

import (
	"github.com/bdkiran/nolan/commitlog"
	"github.com/bdkiran/nolan/utils"
)

func main() {
	utils.LoggerInit(false)
	utils.Info.Println("Nolan Starting up...")
	cl, _ := commitlog.New("logs/partition")
	cl.Append([]byte("We made it"))
	cl.Append([]byte("Dont hate it"))
	cl.Append([]byte("Another test"))
	cl.ReadAll()
}
