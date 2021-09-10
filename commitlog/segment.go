package commitlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	logger "github.com/bdkiran/nolan/utils"
)

const (
	logSuffix   = ".log"
	indexSuffix = ".index"
	fileFormat  = "%05d%s"
)

type segment struct {
	writer         io.Writer
	reader         io.Reader
	log            *os.File
	index          *index
	path           string
	position       int
	maxBytes       int
	startingOffset int
	nextOffset     int
	file           string
}

func newSegment(directory string) (*segment, error) {
	//Starting and lastest start in same place...
	seg := &segment{
		maxBytes:       1000,
		position:       0,
		startingOffset: 0,
		nextOffset:     0,
		file:           directory,
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
		path:           logPath,
		maxBytes:       1000,
		startingOffset: 0,
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
	logger.Info.Println("opening file", ind.path)
	indder, err := os.OpenFile(ind.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return seg, err
	}
	ind.indexFile = indder

	seg.index = ind

	if ind.indexFile == nil {
		logger.Error.Println("Pointer is nil")
		return seg, errors.New("pointer to file is nil")
	}

	totalEntries := seg.index.loadIndex()
	seg.nextOffset = totalEntries

	return seg, nil
}

func (seg *segment) write(message []byte) (int, error) {
	numOfBytes, err := seg.writer.Write(message)
	if err != nil {
		return numOfBytes, err
	}
	seg.index.addEntry(seg.position, numOfBytes)

	seg.position += numOfBytes
	seg.nextOffset++

	return numOfBytes, nil
}

func (s *segment) ReadAt(offset int) (n int, err error) {
	if offset >= s.nextOffset {
		return 0, errors.New("offset out of bounds")
	} else {
		ent := s.index.entries[offset]
		buff := make([]byte, ent.Total)
		s.log.ReadAt(buff, int64(ent.Start))
		logger.Info.Println("Reading segment: ", string(buff))
	}
	return 0, nil
}

func (seg *segment) read(offset int64, total int32) (string, error) {
	_, err := seg.log.Seek(offset, 0)
	if err != nil {
		logger.Error.Println(err)
		return "", err
	}
	b2 := make([]byte, total)
	n2, err := seg.reader.Read(b2)
	if err != nil {
		logger.Error.Println(err)
		return "", err
	}
	return string(b2[:n2]), nil
}

func (seg *segment) readAll() error {
	if seg.log == nil {
		logger.Error.Println("Pointer is nil")
		return errors.New("pointer to file is nil")
	}

	for _, ent := range seg.index.entries {
		_, err := seg.log.Seek(int64(ent.Start), 0)
		if err != nil {
			logger.Error.Println(err)
			return err
		}
		b2 := make([]byte, ent.Total)
		n2, err := seg.reader.Read(b2)
		if err != nil {
			logger.Error.Println(err)
			return err
		}
		logger.Info.Println("Reading segment: ", string(b2[:n2]))
	}

	return nil
}

func (s *segment) logPath() string {
	//TODO: Change from position to something else?
	return filepath.Join(s.file, fmt.Sprintf(fileFormat, s.startingOffset, logSuffix))
}

func (s *segment) indexPath() string {
	//TODO: Change from position to something else?
	return filepath.Join(s.file, fmt.Sprintf(fileFormat, s.startingOffset, indexSuffix))
}
