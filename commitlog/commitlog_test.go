package commitlog

import (
	"os"
	"testing"

	logger "github.com/bdkiran/nolan/utils"
)

const (
	testDirectory = "testPartition"
)

func TestMain(m *testing.M) {
	// Initialize logger so stuff doesnt break
	logger.LoggerInit(false)
	code := m.Run()
	os.Exit(code)
}

// Tests if a new comitlog directory is created
func TestNewCommitlog(t *testing.T) {
	_, err := New(testDirectory)
	if err != nil {
		t.Error("Error when creating new directory.", err)
	}
	//verify that the path was created
	_, err = os.Stat(testDirectory)
	if err != nil {
		t.Error("Directory was not created.", err)
	}
	//clean up
	err = os.Remove(testDirectory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}

//Tests if an existing commitlog will be used.
func TestExistingCommitlog(t *testing.T) {
	err := os.Mkdir(testDirectory, 0755)
	if err != nil {
		t.Error("Error when trying to create a new directory.", err)
	}
	_, err = New(testDirectory)
	if err != nil {
		t.Error("Error when loading a directory.", err)
	}
	//clean up
	err = os.Remove(testDirectory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}

// Create log and index files
// Test that non-matching log and index files get cleaned up
func TestLoadSegments(t *testing.T) {
	//Create a directory
	err := os.Mkdir(testDirectory, 0755)
	if err != nil {
		t.Error("Error when trying to create a new directory.", err)
	}

	//Create logs and indexs
	_, err = newSegment(testDirectory)
	if err != nil {
		t.Error("Error when creating a new segment.", err)
	}

	cl, err := New(testDirectory)
	if err != nil {
		t.Error("Error when creating commitlog.", err)
	}
	expected := 1

	if len(cl.segments) != expected {
		t.Errorf("Expected %d segments but instead got %d segments!", expected, len(cl.segments))
	}

	//clean up
	err = os.RemoveAll(testDirectory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}

func TestLoadSegmentsNoLog(t *testing.T) {
	//Create a directory
	err := os.Mkdir(testDirectory, 0755)
	if err != nil {
		t.Error("Error when trying to create a new directory.", err)
	}

	//Create logs and indexs
	_, err = newSegment(testDirectory)
	if err != nil {
		t.Error("Error when creating a new segment.", err)
	}

	_, err = New(testDirectory)
	if err != nil {
		t.Error("Error when creating commitlog.", err)
	}

	//clean up
	err = os.RemoveAll(testDirectory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}
func TestSplit(t *testing.T) {

}
