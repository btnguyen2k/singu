package singu

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

// New queue instance, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 0 (or not supported)
//	- Orphan Message list must be empty
func test_Empty(test string, queue IQueue, t *testing.T) {
	if msg, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg != nil {
		t.Fatalf("%s failed: expected nil but received %#v", test, msg)
	}

	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}

	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
func test_QueueOne(test string, queue IQueue, t *testing.T) {
	content := "Queue content"
	msg := NewQueueMessage([]byte(content))

	if err := queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
func test_QueueAndTakeOne(test string, queue IQueue, t *testing.T) {
	content := "Queue content"
	msg1 := NewQueueMessage([]byte(content))

	if err := queue.Queue(msg1); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
	} else if msg1.Id != msg2.Id || !bytes.Equal(msg1.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg1.Id, string(msg1.Payload), msg2.Id, string(msg2.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, ephemeralSize)
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
		} else if orphanMsgs[0].Id != msg1.Id || !bytes.Equal(msg1.Payload, orphanMsgs[0].Payload) {
			t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg1.Id, string(msg1.Payload), orphanMsgs[0].Id, string(orphanMsgs[0].Payload))
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
func test_QueueTakeAndFinishOne(test string, queue IQueue, t *testing.T) {
	content := "Queue content"
	msg1 := NewQueueMessage([]byte(content))

	if err := queue.Queue(msg1); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
	} else if msg1.Id != msg2.Id || !bytes.Equal(msg1.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg1.Id, string(msg1.Payload), msg2.Id, string(msg2.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, ephemeralSize)
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
		} else if orphanMsgs[0].Id != msg1.Id || !bytes.Equal(msg1.Payload, orphanMsgs[0].Payload) {
			t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg1.Id, string(msg1.Payload), orphanMsgs[0].Id, string(orphanMsgs[0].Payload))
		}
	}

	if err := queue.Finish(msg1.Id); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else {
		if queueSize, err := queue.QueueSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if queueSize != 0 && queueSize != SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
		}
		if ephemeralSize, err := queue.EphemeralSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
//	- Ephemeral size = 0 (or not supported)
//
// Call IQueue.Finish, expected:
//	- Queue size = 0 (or not supported)
//	- Ephemeral size = 0 (or not supported)
func test_EphemeralDisabled(test string, queue IQueue, t *testing.T) {
	content := "Queue content"
	msg1 := NewQueueMessage([]byte(content))

	if err := queue.Queue(msg1); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}

	if msg2, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg2 == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg1.Id != msg2.Id || !bytes.Equal(msg1.Payload, msg2.Payload) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, msg1.Id, string(msg1.Payload), msg2.Id, string(msg2.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}

	if err := queue.Finish(msg1.Id); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else {
		if queueSize, err := queue.QueueSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if queueSize != 0 && queueSize != SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
		}
		if ephemeralSize, err := queue.EphemeralSize(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
			t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
func test_EphemeralMaxSize(test string, queue IQueue, t *testing.T) {
	ephemeralCapacity, err := queue.EphemeralStorageCapacity()
	if err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if ephemeralCapacity <= 0 {
		t.Fatalf("%s failed: ephemeral storage capacity is unknown", test)
	}

	for i := 0; i <= ephemeralCapacity; i++ {
		content := "Queue content " + strconv.Itoa(i)
		msg := NewQueueMessage([]byte(content))
		msg.Id = strconv.Itoa(i)
		if err := queue.Queue(msg); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != ephemeralCapacity+1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, ephemeralCapacity, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}

	for i := 0; i < ephemeralCapacity; i++ {
		if msg, err := queue.Take(); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		} else if msg == nil {
			t.Fatalf("%s failed: expected message but received nil", test)
		} else if msg.Id != strconv.Itoa(i) || !bytes.Equal(msg.Payload, []byte("Queue content "+strconv.Itoa(i))) {
			t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, strconv.Itoa(i), "Queue content "+strconv.Itoa(i), msg.Id, string(msg.Payload))
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != ephemeralCapacity && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, ephemeralCapacity, SizeNotSupported, ephemeralSize)
	}

	if _, err := queue.Take(); err != ErrorEphemeralIsFull {
		t.Fatalf("%s failed: expected %v but received %v", test, ErrorEphemeralIsFull, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != ephemeralCapacity && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}

	for i := 0; i < ephemeralCapacity; i++ {
		if err := queue.Finish(strconv.Itoa(i)); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}

	if msg, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if msg == nil {
		t.Fatalf("%s failed: expected message but received nil", test)
	} else if msg.Id != strconv.Itoa(ephemeralCapacity) || !bytes.Equal(msg.Payload, []byte("Queue content "+strconv.Itoa(ephemeralCapacity))) {
		t.Fatalf("%s failed: expected [%s/%s] but received [%s/%s]", test, strconv.Itoa(ephemeralCapacity), string("Queue content "+strconv.Itoa(ephemeralCapacity)), msg.Id, string(msg.Payload))
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, ephemeralSize)
	}

	if err := queue.Finish(strconv.Itoa(ephemeralCapacity)); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != 0 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
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
func test_QueueMaxSize(test string, queue IQueue, t *testing.T) {
	queueCapacity, err := queue.QueueStorageCapacity()
	if err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueCapacity <= 0 {
		t.Fatalf("%s failed: queue storage capacity is unknown", test)
	}

	for i := 0; i < queueCapacity; i++ {
		content := "Queue content " + strconv.Itoa(i)
		msg := NewQueueMessage([]byte(content))
		msg.Id = strconv.Itoa(i)
		if err := queue.Queue(msg); err != nil {
			t.Fatalf("%s failed with error: %e", test, err)
		}
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != queueCapacity && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, queueCapacity, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}

	content := "Queue content " + strconv.Itoa(queueCapacity)
	msg := NewQueueMessage([]byte(content))
	msg.Id = strconv.Itoa(queueCapacity)
	if err := queue.Queue(msg); err != ErrorQueueIsFull {
		t.Fatalf("%s failed: expected %v but received %v", test, ErrorQueueIsFull, err)
	}

	if _, err := queue.Take(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != queueCapacity-1 && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, queueCapacity-1, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 1 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 1, SizeNotSupported, ephemeralSize)
	}

	if err := queue.Queue(msg); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	}
	if queueSize, err := queue.QueueSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if queueSize != queueCapacity && queueSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, queueCapacity, SizeNotSupported, queueSize)
	}
	if ephemeralSize, err := queue.EphemeralSize(); err != nil {
		t.Fatalf("%s failed with error: %e", test, err)
	} else if ephemeralSize != 0 && ephemeralSize != SizeNotSupported {
		t.Fatalf("%s failed: expected %d or %d but received %d", test, 0, SizeNotSupported, ephemeralSize)
	}
}
