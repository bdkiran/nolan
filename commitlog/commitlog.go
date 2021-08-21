package commitlog

import (
	"log"
	"os"
	"strings"
	"sync"
)

type Commitlog struct {
	path     string     //Path to the partition directory
	segments []*segment //Individual segment files
	mu       sync.RWMutex
}

/*
Open will open a commitlog using the provided path.
If no partion already exists, then one will be created.
*/
func New(path string) (*Commitlog, error) {
	cl := Commitlog{
		path: path,
	}
	_, err := os.Stat(path)
	if err != nil {
		//Partitions dont exist, create a new directory and return
		err := os.Mkdir(path, 0755)
		if err != nil {
			log.Fatalln("big problem: ", err)
			return nil, err
		}
		return &cl, nil
	}
	//Since our partition already exists we need to load them in
	cl.loadSegments()
	return &cl, nil
}

func (cl *Commitlog) ReadAll() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	log.Println("Reading total segments: ", len(cl.segments))
	for _, seg := range cl.segments {
		seg.read()
	}
}

func (cl *Commitlog) Append(message []byte) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if len(cl.segments) == 0 {
		log.Println("Creating a new segment")
		segment, err := newSegment(cl.path)
		if err != nil {
			log.Println("Unable to create new segment:", err)
			return err
		}
		cl.segments = append(cl.segments, segment)
	}
	_, err := cl.segments[len(cl.segments)-1].write(message)
	if err != nil {
		return err
	}
	return nil
}

func (cl *Commitlog) loadSegments() {
	files, err := os.ReadDir(cl.path)
	if err != nil {
		log.Fatal("Unable to read directory: ", err)
	}
	var segmentFileName string
	var indexFileName string

	//TODO: Support multiple segments!!
	for _, f := range files {
		if strings.HasSuffix(f.Name(), logSuffix) {
			segmentFileName = cl.path + "/" + f.Name()
		} else if strings.HasSuffix(f.Name(), indexSuffix) {
			indexFileName = cl.path + "/" + f.Name()
		}
	}
	//TODO: Do correct checks on the files
	if indexFileName != "" && segmentFileName != "" {
		seg, err := loadSegment(indexFileName, segmentFileName)
		if err != nil {
			log.Fatal(err)
		}
		cl.segments = append(cl.segments, seg)
	}

}
