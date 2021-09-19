package commitlog

import (
	"os"
	"testing"

	logger "github.com/bdkiran/nolan/utils"
)

func TestMain(m *testing.M) {
	// Initialize logger so stuff doesnt break
	logger.LoggerInit(false)
	code := m.Run()
	os.Exit(code)
}

// Tests if a new comitlog directory is created
func TestNewCommitlog(t *testing.T) {
	directory := "testPartition"
	_, err := New(directory)
	if err != nil {
		t.Error("Error when creating new directory.", err)
	}
	//verify that the path was created
	_, err = os.Stat(directory)
	if err != nil {
		t.Error("Directory was not created.", err)
	}
	//clean up
	err = os.Remove(directory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}

//Tests if an existing commitlog will be used.
func TestExistingCommitlog(t *testing.T) {
	directory := "testPartition"
	err := os.Mkdir(directory, 0755)
	if err != nil {
		t.Error("Error when trying to create a new directory.", err)
	}
	_, err = New(directory)
	if err != nil {
		t.Error("Error when loading a directory.", err)
	}
	//clean up
	err = os.Remove(directory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}

// Create log and index files
// Test that non-matching log and index files get cleaned up
func TestLoadSegments(t *testing.T) {
	//Slice of logs
	//Slice of indexs
	directory := "testPartition"
	_, err := New(directory)
	if err != nil {
		t.Error("Error when creating new directory.", err)
	}
}
