package commitlog

import (
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	logger "github.com/bdkiran/nolan/utils"
)

type Commitlog struct {
	path            string     //Path to the partition directory
	segments        []*segment //Individual segment files
	mu              sync.RWMutex
	vCurrentSegment atomic.Value
}

/*
	New will open a commitlog using the provided path.
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

/*
	Append appends a new entry to the commitlog
*/
func (cl *Commitlog) Append(message []byte) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if len(cl.segments) == 0 {
		logger.Info.Println("Creating a new segment")
		segment, err := newSegment(cl.path, 0)
		if err != nil {
			logger.Error.Println("Unable to create new segment:", err)
			cl.mu.Unlock()
			return err
		}
		cl.segments = append(cl.segments, segment)
		cl.vCurrentSegment.Store(segment)
	}
	curSegment := cl.getCurrentSegment()
	_, err := curSegment.write(message)
	if err != nil {
		if err.Error() == "max segment length" {
			cl.mu.Unlock()
			//Check for error if too many bytes in the segment -> then split
			err = cl.split()
			if err != nil {
				return err
			}
			//Append again, this time on the new segment...
			cl.Append(message)
			cl.mu.Lock() //So defer unlock works..
			return nil
		}
		return err
	}
	return nil
}

/*
	loadSegments loads all of the segments from disk into memory to read
*/
func (cl *Commitlog) loadSegments() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	files, err := os.ReadDir(cl.path)
	if err != nil {
		logger.Error.Fatal("Unable to read directory: ", err)
	}

	filesStrings := []string{}
	filesToRemove := []string{}
	for _, file := range files {
		filesStrings = append(filesStrings, file.Name())
	}
	sort.Strings(filesStrings)

	//TODO: Support multiple segments!!
	for _, file := range files {
		if strings.HasSuffix(file.Name(), logSuffix) {
			//Log file
			corespondingIndexFile := strings.Replace(file.Name(), logSuffix, indexSuffix, 1)
			isPresentValue := sort.SearchStrings(filesStrings, corespondingIndexFile)
			if isPresentValue == len(filesStrings) {
				filesToRemove = append(filesToRemove, file.Name())
			}
			// else {
			//This will not work if the path is passed in as "path/to/"
			// logfileToLoad := cl.path + "/" + file.Name()
			// indexFileToLoad := cl.path + "/" + corespondingIndexFile
			// seg, err := loadSegment(indexFileToLoad, logfileToLoad)
			// if err != nil {
			// 	logger.Error.Fatal(err)
			// }
			// cl.segments = append(cl.segments, seg)
			// }

		} else if strings.HasSuffix(file.Name(), indexSuffix) {
			corespondingLogFile := strings.Replace(file.Name(), indexSuffix, logSuffix, 1)
			isPresentValue := sort.SearchStrings(filesStrings, corespondingLogFile)
			if isPresentValue == len(filesStrings) {
				if err := os.Remove(file.Name()); err != nil {
					return err
				}
			} else {
				logfileToLoad := cl.path + "/" + corespondingLogFile
				indexFileToLoad := cl.path + "/" + file.Name()
				seg, err := loadSegment(indexFileToLoad, logfileToLoad)
				if err != nil {
					logger.Error.Fatal(err)
				}
				cl.segments = append(cl.segments, seg)
			}
		}
	}
	//Delete all of the corrupted files...
	//Should this be in a goroutine??
	for _, file := range filesToRemove {
		fullFilePath := cl.path + "/" + file
		if err := os.Remove(fullFilePath); err != nil {
			return err
		}
	}
	//If there are no segments, then we cannot set an active segment
	if len(cl.segments) > 0 {
		//Set our active segment to the latest segment
		latestSegment := cl.segments[len(cl.segments)-1]
		cl.vCurrentSegment.Store(latestSegment)
	}

	return nil
}

func (cl *Commitlog) split() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	seg := cl.getCurrentSegment()
	segment, err := newSegment(cl.path, seg.nextOffset)
	if err != nil {
		return err
	}

	logger.Info.Println(segment.path)

	cl.segments = append(cl.segments, segment)
	//Set our current segment to the new segment created
	cl.vCurrentSegment.Store(segment)

	return nil
}

func (l *Commitlog) getCurrentSegment() *segment {
	return l.vCurrentSegment.Load().(*segment)
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
	Read will read a specific 'offset' within the commitlog,
	if the offset is out of bounds returns error
*/
func (cl *Commitlog) Read(offset int) ([]byte, error) {
	logger.Info.Println("Reading...")
	cl.mu.Lock()
	defer cl.mu.Unlock()

	var segmentContainsOffset *segment
	// Get the segment that holds the offset
	for _, seg := range cl.segments {
		if seg.startingOffset <= offset {
			segmentContainsOffset = seg
		} else {
			break
		}
	}

	logger.Info.Println(segmentContainsOffset.path)
	//TODO: Load correct segment, not just the newest segment
	//newestSeg := cl.segments[len(cl.segments)-1]

	buff, err := segmentContainsOffset.readAt(offset)
	if err != nil {
		logger.Error.Println(err)
		return nil, err
	}
	return buff, err
}

/*************** Helper/Debugger functions....*************************/

/*
	Not sure how useful this actually is.. Not really used at the moment..
*/
// func (cl *Commitlog) ReadLatestEntry() {
// 	cl.mu.Lock()
// 	defer cl.mu.Unlock()
// 	newestSeg := cl.segments[len(cl.segments)-1]
// 	lastEntry := newestSeg.index.entries[len(newestSeg.index.entries)-1]
// 	latest, err := newestSeg.read(int64(lastEntry.Start), lastEntry.Total)
// 	if err != nil {
// 		logger.Error.Println(err)
// 	}
// 	logger.Info.Println(latest)
// }
