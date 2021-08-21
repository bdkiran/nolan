package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
)

type index struct {
	entries   []entry
	path      string
	indexFile *os.File
}

func (ind *index) addEntry(position int, totalBytes int) {
	ent := entry{
		Start: int32(position),
		Total: int32(totalBytes),
	}
	ind.entries = append(ind.entries, ent)
	log.Println(ent)
	b := new(bytes.Buffer)
	if err := binary.Write(b, binary.BigEndian, ent); err != nil {
		log.Panicln(err)
	}
	log.Println(b.Bytes())
	log.Println(len(b.Bytes()))

	ind.indexFile.Write(b.Bytes())
}

func (ind *index) loadIndex() {
	log.Println("Reading index..")
	if ind.indexFile == nil {
		log.Println("Pointer is nil")
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
				log.Fatal("Unexpected read error: ", err)
			}
		}
		buffer := bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.BigEndian, &ent)
		if err != nil {
			log.Fatal("binary read failed", err)
		}
		ind.entries = append(ind.entries, ent)
	}
	log.Println(len(ind.entries))
	log.Printf("%v\n", ind.entries)
	//log.Printf("%v\n", ind)
}

type entry struct {
	Start int32
	Total int32
}
