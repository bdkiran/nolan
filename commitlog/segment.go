package commitlog

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"
	fileFormat  = "%010d%s"
)

type segment struct {
	writer        io.Writer
	reader        io.Reader
	log           *os.File
	index         *index
	path          string
	position      int
	maxBytes      int
	partitionPath string
}

func newSegment(directory string) (*segment, error) {
	seg := &segment{
		maxBytes:      1000,
		position:      0,
		partitionPath: directory,
	}

	seg.path = seg.logPath()
	loggly, err := os.OpenFile(seg.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return seg, err
	}
	seg.log = loggly
	seg.reader = loggly
	seg.writer = loggly

	//Handle index creation
	ind := &index{
		path: seg.indexPath(),
	}
	indder, err := os.OpenFile(ind.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return seg, err
	}
	ind.indexFile = indder
	//Add index pointer to our segment
	seg.index = ind

	return seg, nil
}

func loadSegment(indexPath string, logPath string) (*segment, error) {
	seg := &segment{
		path:     logPath,
		maxBytes: 1000,
	}
	loggly, err := os.OpenFile(seg.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return seg, err
	}
	seg.log = loggly
	seg.reader = loggly
	seg.writer = loggly

	fi, err := loggly.Stat()
	if err != nil {
		return seg, err
	}
	seg.position = int(fi.Size())

	ind := &index{
		path: indexPath,
	}
	log.Println("opening file", ind.path)
	indder, err := os.OpenFile(ind.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return seg, err
	}
	ind.indexFile = indder

	seg.index = ind

	if ind.indexFile == nil {
		log.Println("Pointer is nil")
		return seg, errors.New("pointer to file is nil")
	}
	return seg, nil
}

func (seg *segment) write(message []byte) (int, error) {
	numOfBytes, err := seg.writer.Write(message)
	if err != nil {
		return numOfBytes, err
	}
	seg.index.addEntry(seg.position, numOfBytes)

	seg.position += numOfBytes
	return numOfBytes, nil
}

func (seg *segment) read() (string, error) {
	if seg.log == nil {
		log.Println("Pointer is nil")
		return "", errors.New("pointer to file is nil")
	}
	seg.index.loadIndex()
	for _, ent := range seg.index.entries {
		_, err := seg.log.Seek(int64(ent.Start), 0)
		if err != nil {
			log.Println("Here", err)
			return "", err
		}
		b2 := make([]byte, ent.Total)
		n2, err := seg.reader.Read(b2)
		if err != nil {
			log.Println("here2", err)
			return "", err
		}
		log.Println("Reading segment: ", string(b2[:n2]))
	}

	return "", nil
}

func (s *segment) logPath() string {
	//TODO: Change from position to something else?
	return filepath.Join(s.partitionPath, fmt.Sprintf(fileFormat, s.position, logSuffix))
}

func (s *segment) indexPath() string {
	//TODO: Change from position to something else?
	return filepath.Join(s.partitionPath, fmt.Sprintf(fileFormat, s.position, indexSuffix))
}
