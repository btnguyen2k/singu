package test

import (
	"bytes"
	"fmt"
	"github.com/btnguyen2k/singu"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// New queue instance, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//	- Orphan Message list must be empty
func MyTest_Empty(test string, queue singu.IQueue, t *testing.T) {
	if msg, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg != nil {
		t.Fatalf("%s failed: expected nil but received %#v", test, msg)
	}

	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}

	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	} else if ephemeralSize >= 0 {
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}
	}
}

// Queue one message, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//	- Orphan message list must be empty
func MyTest_QueueOne(test string, queue singu.IQueue, t *testing.T) {
	content := "Queue content"
	msg := singu.NewQueueMessage([]byte(content))

	if _, err := queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	} else if ephemeralSize >= 0 {
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}
	}
}

// Queue one message, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//	- Orphan message list must be empty
//
// Take one message from queue, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 1 (or not supported)
//	- Orphan message list (long period) must be empty
//	- Orphan message list (short period) must contain 1 item
func MyTest_QueueAndTakeOne(test string, queue singu.IQueue, t *testing.T) {
	content := "Queue content"
	msg := singu.NewQueueMessage([]byte(content))
	var err error
	if msg, err = queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	} else if ephemeralSize >= 0 {
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}
	}

	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != msg2.Id || !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), msg2.Id, string(msg2.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	} else if ephemeralSize > 0 {
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}

		time.Sleep(3 * time.Second)

		if orphanMsgs, err := queue.OrphanMessages(2); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 1 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 1, 2, len(orphanMsgs))
		} else if orphanMsgs[0].Id != msg.Id || !bytes.Equal(msg.Payload, orphanMsgs[0].Payload) {
			t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), orphanMsgs[0].Id, string(orphanMsgs[0].Payload))
		}
	}
}

// Queue one message, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//	- Orphan message list must be empty
//
// Take one message from queue, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 1 (or not supported)
//	- Orphan message list (long period) must be empty
//	- Orphan message list (short period) must contain 1 item
//
// Call IQueue.Finish, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//	- Orphan message list must be empty
func MyTest_QueueTakeAndFinishOne(test string, queue singu.IQueue, t *testing.T) {
	content := "Queue content"
	msg := singu.NewQueueMessage([]byte(content))
	var err error
	if msg, err = queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	} else if ephemeralSize >= 0 {
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}
	}

	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != msg2.Id || !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), msg2.Id, string(msg2.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	} else if ephemeralSize > 0 {
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}

		time.Sleep(3 * time.Second)

		if orphanMsgs, err := queue.OrphanMessages(2); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 1 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 1, 2, len(orphanMsgs))
		} else if orphanMsgs[0].Id != msg.Id || !bytes.Equal(msg.Payload, orphanMsgs[0].Payload) {
			t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), orphanMsgs[0].Id, string(orphanMsgs[0].Payload))
		}
	}

	if err := queue.Finish(msg.Id); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else {
		if queueSize, err := queue.QueueSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
		}
		if ephemeralSize, err := queue.EphemeralSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
		}
		if orphanMsgs, err := queue.OrphanMessages(10); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if len(orphanMsgs) != 0 {
			t.Fatalf("%s failed: expected %d orpham messages (in %d seconds) received %d ones", test, 0, 10, len(orphanMsgs))
		}
	}
}

// Queue one message, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Take one message from queue, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 1 (or not supported)
//
// Call IQueue.Requeue(msg, false), expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Take one message from queue, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 1 (or not supported)
//	- Message's re-queue count is 1
//
// Call IQueue.Requeue(msg, true), expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Take one message from queue, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 1 (or not supported)
//	- Message's re-queue count remains 1
func MyTest_QueueTakeAndRequeueOne(test string, queue singu.IQueue, t *testing.T) {
	content := "Queue content"
	msg := singu.NewQueueMessage([]byte(content))
	var err error

	// queue
	if msg, err = queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	// take
	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != msg2.Id || !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), msg2.Id, string(msg2.Payload))
	} else if msg.NumRequeues != 0 {
		t.Fatalf("%s failed: expected %d but received %d", test, 0, msg2.NumRequeues)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	}

	// requeue(false)
	if msg, err = queue.Requeue(msg.Id, false); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else {
		if queueSize, err := queue.QueueSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
		}
		if ephemeralSize, err := queue.EphemeralSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
		}
	}

	// take
	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != msg2.Id || !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), msg2.Id, string(msg2.Payload))
	} else if msg2.NumRequeues != 1 {
		t.Fatalf("%s failed: expected %d but received %d", test, 1, msg2.NumRequeues)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	}

	// requeue(true)
	if msg, err = queue.Requeue(msg.Id, true); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else {
		if queueSize, err := queue.QueueSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
		}
		if ephemeralSize, err := queue.EphemeralSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
		}
	}

	// take
	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != msg2.Id || !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), msg2.Id, string(msg2.Payload))
	} else if msg2.NumRequeues != 1 {
		t.Fatalf("%s failed: expected %d but received %d", test, 1, msg2.NumRequeues)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	}
}

// Queue one message, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Take one message from queue, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Call IQueue.Finish, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 0 (or not supported)
func MyTest_EphemeralDisabled(test string, queue singu.IQueue, t *testing.T) {
	content := "Queue content"
	msg := singu.NewQueueMessage([]byte(content))
	var err error

	if msg, err = queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != msg2.Id || !bytes.Equal(msg.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg.Id, string(msg.Payload), msg2.Id, string(msg2.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	if err := queue.Finish(msg.Id); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else {
		if queueSize, err := queue.QueueSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
		}
		if ephemeralSize, err := queue.EphemeralSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
		}
	}
}

// Queue <ephemeral-max-size>+1 messages, expected:
//	- Queue size = <ephemeral-max-size>+1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Take <ephemeral-max-size> messages from queue, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = <ephemeral-max-size> (or not supported)
//
// Take one more message, expected:
//	- ErrorEphemeralIsFull is returned
//
// Finish messages, expected:
//	- Queue size = 1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
func MyTest_EphemeralMaxSize(test string, queue singu.IQueue, t *testing.T) {
	ephemeralCapacity, err := queue.EphemeralStorageCapacity()
	if err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if ephemeralCapacity <= 0 {
		t.Fatalf("%s failed: ephemeral storage capacity is unknown", test)
	}

	var queuedMsgs = make([]*singu.QueueMessage, 0)

	// queue ephemeralCapacity+1 messages
	for i := 0; i <= ephemeralCapacity; i++ {
		content := "Queue content " + strconv.Itoa(i)
		msg := singu.NewQueueMessage([]byte(content))
		if msg, err := queue.Queue(msg); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else {
			queuedMsgs = append(queuedMsgs, msg)
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != ephemeralCapacity+1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, ephemeralCapacity, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	// take ephemeralCapacity messages from  queue
	for i := 0; i < ephemeralCapacity; i++ {
		if msg, err := queue.Take(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if msg == nil {
			t.Fatalf("%s failed: expected message but received nil", test)
		} else if msg.Id != queuedMsgs[i].Id || !bytes.Equal(msg.Payload, []byte("Queue content "+strconv.Itoa(i))) {
			t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, queuedMsgs[i].Id, "Queue content "+strconv.Itoa(i), msg.Id, string(msg.Payload))
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != ephemeralCapacity && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, ephemeralCapacity, singu.SizeNotSupported, ephemeralSize)
	}

	// take one more message from queue
	if _, err := queue.Take(); err != singu.ErrorEphemeralIsFull {
		t.Fatalf("%s failed: expected %v but received %v", test, singu.ErrorEphemeralIsFull, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != ephemeralCapacity && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	// finish ephemeralCapacity messages
	for i := 0; i < ephemeralCapacity; i++ {
		if err := queue.Finish(queuedMsgs[i].Id); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	// take last message from queue
	if msg, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != queuedMsgs[ephemeralCapacity].Id || !bytes.Equal(msg.Payload, []byte("Queue content "+strconv.Itoa(ephemeralCapacity))) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, queuedMsgs[ephemeralCapacity].Id, "Queue content "+strconv.Itoa(ephemeralCapacity), msg.Id, string(msg.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	}

	// finish last message
	if err := queue.Finish(queuedMsgs[ephemeralCapacity].Id); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}
}

// Queue <queue-max-size> messages, expected:
//	- Queue size = <queue-max-size>+1 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//
// Queue one more message, expected:
//	- ErrorQueueIsFull is return
//
// Take a message from queue, expected:
//	- Can queue one more message
func MyTest_QueueMaxSize(test string, queue singu.IQueue, t *testing.T) {
	queueCapacity, err := queue.QueueStorageCapacity()
	if err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueCapacity <= 0 {
		t.Fatalf("%s failed: queue storage capacity is unknown", test)
	}

	for i := 0; i < queueCapacity; i++ {
		content := "Queue content " + strconv.Itoa(i)
		msg := singu.NewQueueMessage([]byte(content))
		msg.Id = fmt.Sprintf("%09d", i)
		if _, err := queue.Queue(msg); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != queueCapacity && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, queueCapacity, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, singu.SizeNotSupported, ephemeralSize)
	}

	content := "Queue content " + strconv.Itoa(queueCapacity)
	msg := singu.NewQueueMessage([]byte(content))
	msg.Id = fmt.Sprintf("%09d", queueCapacity)
	if _, err := queue.Queue(msg); err != singu.ErrorQueueIsFull {
		t.Fatalf("%s failed: expected %v but received %v", test, singu.ErrorQueueIsFull, err)
	}

	if _, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != queueCapacity-1 && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, queueCapacity-1, singu.SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, singu.SizeNotSupported, ephemeralSize)
	}

	if _, err := queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != queueCapacity && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, queueCapacity, singu.SizeNotSupported, queueSize)
	}
}

// Queue a large number of messages using multiple producers. Then take all message using multiple consumers. Expected:
//	- All messages are queued and taken correctly
func MyTest_LongQueue(test string, queue singu.IQueue, numProducers, numConsumers, numMsgs int, t *testing.T) {
	numMsgsPerProducers := numMsgs / numProducers
	numMsgs = numMsgsPerProducers * numProducers
	var wg sync.WaitGroup

	var msgs sync.Map
	wg.Add(numProducers)
	for i := 0; i < numProducers; i++ {
		go func(id int, wg *sync.WaitGroup, numMsgs int) {
			for i := 0; i < numMsgs; i++ {
				content := "Queue content " + strconv.Itoa(id) + "  -  " + strconv.Itoa(i)
				msg := singu.NewQueueMessage([]byte(content))
				if msg, err := queue.Queue(msg); err != nil {
					t.Fatalf("%s failed with error: %e", test, err)
				} else {
					msgs.Store(msg.Id, msg)
				}
			}
			wg.Done()
		}(i, &wg, numMsgsPerProducers)
	}
	wg.Wait()

	var countMsgs = 0
	msgs.Range(func(key, value interface{}) bool {
		countMsgs++
		return true
	})
	fmt.Printf("\tNum produced messages: %d\n", countMsgs)

	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != numMsgs && queueSize != singu.SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, numMsgs, singu.SizeNotSupported, queueSize)
	}

	var numReceived int64 = 0
	wg.Add(numConsumers)
	t1 := time.Now()
	for i := 0; i < numConsumers; i++ {
		go func(wg *sync.WaitGroup, queue singu.IQueue, numReceived *int64) {
			var errorCount = 0
			for msg, err := queue.Take(); (err == nil && msg != nil) || errorCount < 2; msg, err = queue.Take() {
				if err == nil && msg != nil {
					atomic.AddInt64(numReceived, 1)
					errorCount = 0
				} else {
					errorCount++
					time.Sleep(1 * time.Millisecond)
				}
			}
			wg.Done()
		}(&wg, queue, &numReceived)
	}
	wg.Wait()
	t2 := time.Now()
	d := float64(t2.UnixNano()-t1.UnixNano()) / 1000000.0
	r := float64(numReceived) * 1000.0 / d
	fmt.Printf("[%s] Received %d messages in %0.2f ms (%0.2f msgs/sec)\n", test, numReceived, d, r)

	if numReceived != int64(numMsgs) {
		t.Fatalf("%s failed: expected %d msgs but received %d", test, numMsgs, numReceived)
	}
}

// Queue and Take a number of messages using multiple producers and consumers. Expected:
//	- All messages are queued and taken correctly
func MyTest_MultiThreads(test string, queue singu.IQueue, numProducers, numConsumers, numMsgs int, t *testing.T) {
	numMsgsPerProducers := numMsgs / numProducers
	numMsgs = numMsgsPerProducers * numProducers
	var wg sync.WaitGroup

	var msgsProduced, msgsConsumed sync.Map
	wg.Add(numProducers + numConsumers)

	t1 := time.Now()
	// producer
	for i := 0; i < numProducers; i++ {
		go func(id int, wg *sync.WaitGroup, numMsgs int) {
			for i := 0; i < numMsgs; i++ {
				content := "Queue content " + strconv.Itoa(id) + "  -  " + strconv.Itoa(i)
				msg := singu.NewQueueMessage([]byte(content))
				if msg, err := queue.Queue(msg); err != nil {
					t.Fatalf("%s failed with error: %e", test, err)
				} else {
					msgsProduced.Store(msg.Id, msg)
				}
			}
			wg.Done()
		}(i, &wg, numMsgsPerProducers)
	}
	// consumer
	for i := 0; i < numConsumers; i++ {
		go func(wg *sync.WaitGroup, queue singu.IQueue) {
			var errorCount = 0
			for msg, err := queue.Take(); (err == nil && msg != nil) || errorCount < 100; msg, err = queue.Take() {
				if err == nil && msg != nil {
					msgsConsumed.Store(msg.Id, msg)
					errorCount = 0
				} else {
					errorCount++
					time.Sleep(1 * time.Millisecond)
				}
			}
			wg.Done()
		}(&wg, queue)
	}
	wg.Wait()
	t2 := time.Now()

	var countMsgsProduced = 0
	msgsProduced.Range(func(key, value interface{}) bool {
		countMsgsProduced++
		return true
	})

	var countMsgsConsumed = 0
	msgsConsumed.Range(func(key, value interface{}) bool {
		countMsgsConsumed++
		return true
	})

	d := float64(t2.UnixNano()-t1.UnixNano()) / 1000000.0
	r := float64(countMsgsConsumed) * 1000.0 / d
	fmt.Printf("[%s] Produced and Consumed %d messages in %0.2f ms (%0.2f msgs/sec)\n", test, countMsgsConsumed, d, r)

	if countMsgsProduced != numMsgs {
		t.Fatalf("%s failed: expected %d msgs produced but actually %d", test, numMsgs, countMsgsProduced)
	}

	if countMsgsConsumed != numMsgs {
		t.Fatalf("%s failed: expected %d msgs consumed but actually %d", test, numMsgs, countMsgsConsumed)
	}
}
