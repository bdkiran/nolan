package commitlog

import (
	"os"
	"testing"

	logger "github.com/bdkiran/nolan/utils"
)

func TestMain(m *testing.M) {
	logger.LoggerInit(false)
	code := m.Run()
	os.Exit(code)
}

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
