package commitlog

import (
	"os"
	"testing"

	logger "github.com/bdkiran/nolan/utils"
)

func TestNewSegment(t *testing.T) {
	//Create a directory
	err := os.Mkdir(testDirectory, 0755)
	if err != nil {
		t.Error("Error when trying to create a new directory.", err)
	}

	seg, err := newSegment(testDirectory, 0)
	if err != nil {
		t.Error("Error when creating new directory.", err)
	}

	logger.Info.Println(seg.file)

	//verify that the path was created
	_, err = os.Stat(seg.file)
	if err != nil {
		t.Error("Segment log was was not created.", err)
	}

	//clean up
	err = os.RemoveAll(testDirectory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}
