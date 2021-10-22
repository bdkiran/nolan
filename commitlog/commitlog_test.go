package commitlog

import (
	"math/rand"
	"os"
	"testing"

	"github.com/bdkiran/nolan/logger"
)

const (
	testDirectory = "testPartition"
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

/* Functions to Help all of out Testing...*/
func TestMain(m *testing.M) {
	// Initialize logger so stuff doesnt break
	logger.LoggerInit(false)
	code := m.Run()
	os.Exit(code)
}

func generateRandomBytes(length int) []byte {
	buffer := make([]byte, length)
	for i := range buffer {
		buffer[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return buffer
}

func cleanup(t *testing.T, directory string) {
	err := os.RemoveAll(directory)
	if err != nil {
		t.Error("Unable to clean up after test.", err)
	}
}

// Tests if a new comitlog directory is created
func TestNewCommitlog(t *testing.T) {
	_, err := New(testDirectory)
	if err != nil {
		t.Error("Error when creating new directory.", err)
	}
	defer cleanup(t, testDirectory)
	//verify that the path was created
	_, err = os.Stat(testDirectory)
	if err != nil {
		t.Error("Directory was not created.", err)
	}
}

//Tests if an existing commitlog will be used.
func TestExistingCommitlog(t *testing.T) {
	err := os.Mkdir(testDirectory, 0755)
	if err != nil {
		t.Error("Error when trying to create a new directory.", err)
	}
	defer cleanup(t, testDirectory)

	_, err = New(testDirectory)
	if err != nil {
		t.Error("Error when loading a directory.", err)
	}
}

//Test appending a segment
func TestAppendCommitlog(t *testing.T) {
	cl, err := New(testDirectory)
	if err != nil {
		t.Error("Error when loading a directory.", err)
	}
	defer cleanup(t, testDirectory)
	appendString := "hello"
	cl.Append([]byte(appendString))

	buffer, err := cl.Read(len(cl.segments) - 1)
	if err != nil {
		t.Error("Error when reading occured.", err)
	}

	if appendString != string(buffer) {
		t.Errorf("Expected %s message but instead got %s message!", appendString, string(buffer))
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
	defer cleanup(t, testDirectory)

	//Create logs and indexs
	_, err = newSegment(testDirectory, 0)
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
}

//Test appending a segment
func TestSplitCommitlog(t *testing.T) {
	cl, err := New(testDirectory)
	if err != nil {
		t.Error("Error when loading a directory.", err)
	}
	defer cleanup(t, testDirectory)
	//Limit on split is 1000byte, we need to append more than that to split
	for i := 0; i < 10; i++ {
		bytesToAppend := generateRandomBytes(120)
		err = cl.Append(bytesToAppend)
		if err != nil {
			t.Error("Error when loading a directory.", err)
		}
	}

	files, err := os.ReadDir(cl.path)
	if err != nil {
		t.Error("Unable to read directory")
	}

	if len(files) != 4 {
		t.Errorf("Expected 4 files, got %d: ", len(files))
	}
}
