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
