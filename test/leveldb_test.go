package test

import (
	"github.com/btnguyen2k/singu/leveldb"
	"os"
	"testing"
)

var (
	queueNameLeveldb = "leveldb"
	dataPath         = os.TempDir() + "/singu"
)

func TestLeveldbQueue_Empty(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_Empty("TestLeveldbQueue_Empty", queue, t)
}

func TestLeveldbQueue_QueueOne(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_QueueOne("TestLeveldbQueue_QueueOne", queue, t)
}

func TestLeveldbQueue_QueueAndTakeOne(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_QueueAndTakeOne("TestLeveldbQueue_QueueAndTakeOne", queue, t)
}

func TestLeveldbQueue_QueueTakeAndFinishOne(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_QueueTakeAndFinishOne("TestLeveldbQueue_QueueTakeAndFinishOne", queue, t)
}

func TestLeveldbQueue_EphemeralDisabled(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, true, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_EphemeralDisabled("TestLeveldbQueue_EphemeralDisabled", queue, t)
}

func TestLeveldbQueue_EphemeralMaxSize(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 10)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_EphemeralMaxSize("TestLeveldbQueue_EphemeralMaxSize", queue, t)
}

func TestLeveldbQueue_QueueMaxSize(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 10, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_QueueMaxSize("TestLeveldbQueue_QueueMaxSize", queue, t)
}
