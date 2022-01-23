package commitlog

import (
	"github.com/bdkiran/nolan/logger"
)

const RETENTION_BYTES = 2000

func (cl *Commitlog) clean() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cleanedSegments, err := cl.deleteCleaner()
	if err != nil {
		return err
	}
	cl.segments = cleanedSegments
	return nil
}

//Clean by size
func (cl *Commitlog) deleteCleaner() ([]*segment, error) {
	var cleanedSegments []*segment
	if len(cl.segments) == 0 {
		//return custom error...
		return cl.segments, nil
	}

	bytesSize := 0
	var i int
	for i = len(cl.segments) - 1; i >= 0; i-- {
		seg := cl.segments[i]
		if bytesSize > RETENTION_BYTES {
			break
		}
		logger.Info.Println("Keeping: ", seg.path)
		cleanedSegments = append(cleanedSegments, seg)
		bytesSize += seg.position
	}

	for j := 0; j <= i; j++ {
		seg := cl.segments[j]
		logger.Info.Println("Deleting: ", seg.path)
		if err := seg.delete(); err != nil {
			logger.Error.Println("Unable to delete segment: ", seg.path)
			return cleanedSegments, err
		}
	}
	return cleanedSegments, nil
}
