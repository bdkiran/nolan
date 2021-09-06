package commitlog

import (
	"os"
	"strings"
	"sync"

	logger "github.com/bdkiran/nolan/utils"
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
			logger.Error.Fatalln("big problem: ", err)
			return nil, err
		}
		return &cl, nil
	}
	//Since our partition already exists we need to load them in
	cl.loadSegments()
	return &cl, nil
}

func (cl *Commitlog) ReadLatestEntry() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	newestSeg := cl.segments[len(cl.segments)-1]
	lastEntry := newestSeg.index.entries[len(newestSeg.index.entries)-1]
	latest, err := newestSeg.read(int64(lastEntry.Start), lastEntry.Total)
	if err != nil {
		logger.Error.Println(err)
	}
	logger.Info.Println(latest)
}

/*
	Reads everything written to the commit log, probably should be used to just debug
*/
func (cl *Commitlog) ReadAll() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	logger.Info.Println("Reading total segments: ", len(cl.segments))
	for _, seg := range cl.segments {
		seg.readAll()
	}
}

/*
	Append appends a new entry to the commitlog
*/
func (cl *Commitlog) Append(message []byte) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if len(cl.segments) == 0 {
		logger.Info.Println("Creating a new segment")
		segment, err := newSegment(cl.path)
		if err != nil {
			logger.Error.Println("Unable to create new segment:", err)
			return err
		}
		cl.segments = append(cl.segments, segment)
	}
	_, err := cl.segments[len(cl.segments)-1].write(message)
	if err != nil {
		//Check for error if too many bytes in the segment -> then split
		cl.split()
		return err
	}
	return nil
}

/*
	loadSegments loads all of the segments from disk into memory to read
*/
func (cl *Commitlog) loadSegments() {
	files, err := os.ReadDir(cl.path)
	if err != nil {
		logger.Error.Fatal("Unable to read directory: ", err)
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
			logger.Error.Fatal(err)
		}
		cl.segments = append(cl.segments, seg)
	}
}

func (cl *Commitlog) split() {
	logger.Info.Println("Spliting segment")
	//Get the active segment
	// Get the total number of entries
	// pass in the new number of entries

}
