package commitlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

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
	mu             sync.Mutex
}

func newSegment(directory string, offset int) (*segment, error) {
	//Starting and lastest start in same place...
	seg := &segment{
		maxBytes:       1000,
		position:       0,
		startingOffset: offset,
		nextOffset:     offset,
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
	logBase := filepath.Base(logPath)
	offsetStr := strings.TrimSuffix(logBase, logSuffix)
	baseOffset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return nil, err
	}

	loggly, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	seg := &segment{
		path:           logPath,
		maxBytes:       1000,
		startingOffset: baseOffset,
		log:            loggly,
		reader:         loggly,
		writer:         loggly,
	}

	fi, err := loggly.Stat()
	if err != nil {
		return seg, err
	}
	seg.position = int(fi.Size())

	ind := &index{
		path: indexPath,
	}
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
	seg.mu.Lock()
	defer seg.mu.Unlock()
	//Check the byte status..
	fileInfo, err := seg.log.Stat()
	if err != nil {
		return 0, err
	}
	computedSize := fileInfo.Size() + int64(len(message))
	//Strickly greater than
	if computedSize > int64(seg.maxBytes) {
		return 0, errors.New("max segment length")
	}

	numOfBytes, err := seg.writer.Write(message)
	if err != nil {
		return numOfBytes, err
	}
	seg.index.addEntry(seg.position, numOfBytes)

	seg.position += numOfBytes
	seg.nextOffset++

	return numOfBytes, nil
}

func (seg *segment) readAt(offset int) (returnBuff []byte, err error) {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	var buff []byte
	if offset >= seg.nextOffset {
		return nil, errors.New("offset out of bounds")
	} else {
		ent := seg.index.entries[offset]
		buff = make([]byte, ent.Total)
		seg.log.ReadAt(buff, int64(ent.Start))
		logger.Info.Println("Reading segment: ", string(buff))
	}
	return buff, nil
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

/* Unused/Not Implemented Functions */
// func (seg *segment) close() error {
// 	seg.mu.Lock()
// 	defer seg.mu.Unlock()
// 	if err := seg.log.Close(); err != nil {
// 		return err
// 	}
// 	return nil
// }

/* Deprecated functions */

// func (seg *segment) read(offset int64, total int32) (string, error) {
// 	seg.mu.Lock()
// 	defer seg.mu.Unlock()
// 	_, err := seg.log.Seek(offset, 0)
// 	if err != nil {
// 		logger.Error.Println(err)
// 		return "", err
// 	}
// 	b2 := make([]byte, total)
// 	n2, err := seg.reader.Read(b2)
// 	if err != nil {
// 		logger.Error.Println(err)
// 		return "", err
// 	}
// 	return string(b2[:n2]), nil
// }
