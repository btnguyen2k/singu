package test

import (
	"github.com/btnguyen2k/singu"
	"testing"
)

const (
	queueNameInmem = "queue"
)

func TestInmemQueue_Empty(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_Empty("TestInmemQueue_Empty", queue, t)
}

func TestInmemQueue_QueueOne(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_QueueOne("TestInmemQueue_QueueOne", queue, t)
}

func TestInmemQueue_QueueAndTakeOne(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_QueueAndTakeOne("TestInmemQueue_QueueAndTakeOne", queue, t)
}

func TestInmemQueue_QueueTakeAndFinishOne(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_QueueTakeAndFinishOne("TestInmemQueue_QueueTakeAndFinishOne", queue, t)
}

func TestInmemQueue_QueueTakeAndRequeueOne(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_QueueTakeAndRequeueOne("TestInmemQueue_QueueTakeAndRequeueOne", queue, t)
}

func TestInmemQueue_EphemeralDisabled(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, true, 0)
	MyTest_EphemeralDisabled("TestInmemQueue_EphemeralDisabled", queue, t)
}

func TestInmemQueue_EphemeralMaxSize(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 10)
	MyTest_EphemeralMaxSize("TestInmemQueue_EphemeralMaxSize", queue, t)
}

func TestInmemQueue_QueueMaxSize(t *testing.T) {
	queue := singu.NewInmemQueue(queueNameInmem, 10, false, 0)
	MyTest_QueueMaxSize("TestInmemQueue_EphemeralMaxSize", queue, t)
}

func doTestInmemQueue_LongQueue(t *testing.T, name string, numProducers, numConsumers, numMsgs int) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_LongQueue(name, queue, numProducers, numConsumers, numMsgs, t)
}

func TestInmemQueue_LongQueue(t *testing.T) {
	doTestInmemQueue_LongQueue(t, "TestInmemQueue_LongQueue_4P1C", 4, 1, 1000000)
	doTestInmemQueue_LongQueue(t, "TestInmemQueue_LongQueue_1P2C", 1, 2, 1000000)
	doTestInmemQueue_LongQueue(t, "TestInmemQueue_LongQueue_2P4C", 2, 4, 1000000)
	doTestInmemQueue_LongQueue(t, "TestInmemQueue_LongQueue_4P8C", 4, 8, 1000000)
}

func doTestInmemQueue_MultiThreads(t *testing.T, name string, numProducers, numConsumers, numMsgs int) {
	queue := singu.NewInmemQueue(queueNameInmem, 0, false, 0)
	MyTest_MultiThreads(name, queue, numProducers, numConsumers, numMsgs, t)
}

func TestInmemQueue_MultiThreads(t *testing.T) {
	doTestInmemQueue_MultiThreads(t, "TestInmemQueue_MultiThreads_4P1C", 4, 1, 1000000)
	doTestInmemQueue_MultiThreads(t, "TestInmemQueue_MultiThreads_1P2C", 1, 2, 1000000)
	doTestInmemQueue_MultiThreads(t, "TestInmemQueue_MultiThreads_2P4C", 2, 4, 1000000)
	doTestInmemQueue_MultiThreads(t, "TestInmemQueue_MultiThreads_4P8C", 4, 8, 1000000)
}
