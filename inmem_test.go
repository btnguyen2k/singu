package singu

import "testing"

const (
	queueNameInmem = "queue"
)

func TestInmemQueue_Empty(t *testing.T) {
	queue := NewInmemQueue(queueNameInmem, 0, false, 0)
	test_Empty("TestInmemQueue_Empty", queue, t)
}

func TestInmemQueue_QueueOne(t *testing.T) {
	queue := NewInmemQueue(queueNameInmem, 0, false, 0)
	test_QueueOne("TestInmemQueue_QueueOne", queue, t)
}

func TestInmemQueue_QueueAndTakeOne(t *testing.T) {
	queue := NewInmemQueue(queueNameInmem, 0, false, 0)
	test_QueueAndTakeOne("TestInmemQueue_QueueAndTakeOne", queue, t)
}

func TestInmemQueue_QueueTakeAndFinishOne(t *testing.T) {
	queue := NewInmemQueue(queueNameInmem, 0, false, 0)
	test_QueueTakeAndFinishOne("TestInmemQueue_QueueTakeAndFinishOne", queue, t)
}

func TestInmemQueue_EphemeralDisabled(t *testing.T) {
	queue := NewInmemQueue(queueNameInmem, 0, true, 0)
	test_EphemeralDisabled("TestInmemQueue_EphemeralDisabled", queue, t)
}

func TestInmemQueue_EphemeralMaxSize(t *testing.T) {
	queue := NewInmemQueue(queueNameInmem, 0, false, 10)
	test_EphemeralMaxSize("TestInmemQueue_EphemeralMaxSize", queue, t)
}
