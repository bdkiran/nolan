package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/bdkiran/nolan/logger"
)

type index struct {
	entries   []entry
	path      string
	indexFile *os.File
	mu        sync.RWMutex
}

type entry struct {
	Start int32
	Total int32
}

func (ind *index) addEntry(position int, totalBytes int) {
	ind.mu.Lock()
	defer ind.mu.Unlock()
	ent := entry{
		Start: int32(position),
		Total: int32(totalBytes),
	}
	ind.entries = append(ind.entries, ent)
	logger.Info.Println(ent)
	b := new(bytes.Buffer)
	if err := binary.Write(b, binary.BigEndian, ent); err != nil {
		logger.Error.Panicln(err)
	}

	ind.indexFile.Write(b.Bytes())
}

func (ind *index) loadIndex() int {
	logger.Info.Println("Reading index..")
	if ind.indexFile == nil {
		logger.Error.Println("Pointer is nil")
		return 0
	}
	ind.mu.Lock()
	defer ind.mu.Unlock()
	ent := entry{}
	//Set to the begining of the file
	ind.indexFile.Seek(0, 0)
	for {
		data := make([]byte, 8)
		_, err := ind.indexFile.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				logger.Error.Fatal("Unexpected read error: ", err)
			}
		}
		buffer := bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.BigEndian, &ent)
		if err != nil {
			logger.Error.Fatal("binary read failed", err)
		}
		ind.entries = append(ind.entries, ent)
	}
	logger.Info.Println("Total index entries", len(ind.entries))
	//logger.Info.Printf("%v\n", ind.entries)
	return len(ind.entries)
}

/* Not implemented functions... */
// func (ind *index) close() error {
// 	ind.mu.Lock()
// 	defer ind.mu.Unlock()
// 	if err := ind.indexFile.Close(); err != nil {
// 		return err
// 	}
// 	return nil
// }
