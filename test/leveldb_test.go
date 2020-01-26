package test

import (
	"github.com/btnguyen2k/singu/leveldb"
	"os"
	"testing"
)

const queueNameLeveldb = "leveldb"

var dataPath = os.TempDir() + "/singu"

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

func TestLeveldbQueue_QueueTakeAndRequeueOne(t *testing.T) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_QueueTakeAndRequeueOne("TestLeveldbQueue_QueueTakeAndRequeueOne", queue, t)
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

func doTestLeveldbQueue_LongQueue(t *testing.T, name string, numProducers, numConsumers, numMsgs int) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_LongQueue(name, queue, numProducers, numConsumers, numMsgs, t)
}

func TestLeveldbQueue_LongQueue(t *testing.T) {
	doTestLeveldbQueue_LongQueue(t, "TestLeveldbQueue_LongQueue_4P1C", 4, 1, 100000)
	doTestLeveldbQueue_LongQueue(t, "TestLeveldbQueue_LongQueue_1P2C", 1, 2, 100000)
	doTestLeveldbQueue_LongQueue(t, "TestLeveldbQueue_LongQueue_2P4C", 2, 4, 100000)
	doTestLeveldbQueue_LongQueue(t, "TestLeveldbQueue_LongQueue_4P8C", 4, 8, 100000)
}

func doTestLeveldbQueue_MultiThreads(t *testing.T, name string, numProducers, numConsumers, numMsgs int) {
	os.RemoveAll(dataPath + "/" + queueNameLeveldb)
	queue := leveldb.NewLeveldbQueue(queueNameLeveldb, dataPath, 0, false, 0)
	defer queue.(*leveldb.LeveldbQueue).Destroy()
	MyTest_MultiThreads(name, queue, numProducers, numConsumers, numMsgs, t)
}

func TestLeveldbQueue_MultiThreads(t *testing.T) {
	doTestLeveldbQueue_MultiThreads(t, "TestLeveldbQueue_MultiThreads_4P1C", 4, 1, 100000)
	doTestLeveldbQueue_MultiThreads(t, "TestLeveldbQueue_MultiThreads_1P2C", 1, 2, 100000)
	doTestLeveldbQueue_MultiThreads(t, "TestLeveldbQueue_MultiThreads_2P4C", 2, 4, 100000)
	doTestLeveldbQueue_MultiThreads(t, "TestLeveldbQueue_MultiThreads_4P8C", 4, 8, 100000)
}
