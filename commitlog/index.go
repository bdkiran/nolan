package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	logger "github.com/bdkiran/nolan/utils"
)

type index struct {
	entries   []entry
	path      string
	indexFile *os.File
}

type entry struct {
	Start int32
	Total int32
}

func (ind *index) addEntry(position int, totalBytes int) {
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
	logger.Info.Println(b.Bytes())
	logger.Info.Println(len(b.Bytes()))

	ind.indexFile.Write(b.Bytes())
}

func (ind *index) loadIndex() {
	logger.Info.Println("Reading index..")
	if ind.indexFile == nil {
		logger.Error.Println("Pointer is nil")
		return
	}
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
	logger.Info.Println(len(ind.entries))
	logger.Info.Printf("%v\n", ind.entries)
}
