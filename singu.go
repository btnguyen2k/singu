// Package singu defines queue message struct and queue API.
package singu

import (
	"bytes"
	"errors"
	"github.com/btnguyen2k/consu/olaf"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	// Version of singu
	Version = "0.1.1"
)

func getMacAddr() string {
	interfaces, err := net.Interfaces()
	if err == nil {
		for _, i := range interfaces {
			if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {
				// Don't use random as we have a real address
				return i.HardwareAddr.String()
			}
		}
	}
	return ""
}

func getMacAddrAsLong() int64 {
	mac, _ := strconv.ParseInt(strings.Replace(getMacAddr(), ":", "", -1), 16, 64)
	return mac
}

var idGen = olaf.NewOlaf(getMacAddrAsLong())

// UniqueId returns a unique id as string
func UniqueId() string {
	return strings.ToLower(idGen.Id128Hex())
}

// NewQueueMessage creates a new QueueMessage instance with provided payload
func NewQueueMessage(payload []byte) *QueueMessage {
	now := time.Now()
	return &QueueMessage{
		Id:             UniqueId(),
		Timestamp:      now,
		QueueTimestamp: now,
		NumRequeues:    0,
		Payload:        payload,
	}
}

// CloneQueueMessage clones a QueueMessage instance
func CloneQueueMessage(msg QueueMessage) QueueMessage {
	clone := msg
	clone.Payload = []byte(string(msg.Payload))
	return clone
}

// QueueMessage represents a queue message.
type QueueMessage struct {
	Id             string    `json:"id"`           // message's unique id
	Timestamp      time.Time `json:"time"`         // message's creation timestamp
	QueueTimestamp time.Time `json:"qtime"`        // message's last-queued timestamp, maintained by queue implementation
	TakenTimestamp time.Time `json:"ttime"`        // message's taken timestamp, maintained by queue implementation
	NumRequeues    int       `json:"num_requeues"` // how many times message has been re-queued?, maintained by queue implementations
	Payload        []byte    `json:"payload"`      // message's payload
}

var (
	// ErrorOperationNotSupported is returned when the queue implementation does not support the invoked operation
	ErrorOperationNotSupported = errors.New("operation not supported")

	// ErrorQueueIsFull is returned when queue storage is full and can not accept any more message
	ErrorQueueIsFull = errors.New("queue storage is full")

	// ErrorEphemeralIsFull is returned when ephemeral storage is full and can not accept any more message
	ErrorEphemeralIsFull = errors.New("ephemeral storage is full")
)

const (
	// SizeNotSupported is returned if queue implementation does not support counting number of messages in storage
	SizeNotSupported = -1
)

// IQueue defines API to access queue messages.
//
// Queue implementation:
//	- Queue storage to store queue messages. Messages are put to the tail and taken from the head of queue storage in FIFO manner.
//	- Messages taken from queue storage are temporarily stored in ephemeral storage until Finish or Requeue is called.
//	- Ephemeral storage is optional, depends on queue implementation.
//
// Queue usage flow:
//	- Create a IQueue instance.
//	- Call IQueue.queue(msg) to put messages to queue.
//	- Call IQueue.take() to take messages from queue.
//	- Do something with the message.
//		- When done, call IQueue.finish(id)
//		- If not done and the message needs to be re-queued, call IQueue.requeue(id, true/false) to put the message back to queue.
type IQueue interface {
	// Name returns queue's name.
	Name() string

	// QueueStorageCapacity returns max number of message queue storage can hold, or SizeNotSupported if queue storage has unlimited capacity.
	QueueStorageCapacity() (int, error)

	// EphemeralStorageCapacity returns max number of message ephemeral storage can hold, or SizeNotSupported if ephemeral storage has unlimited capacity.
	EphemeralStorageCapacity() (int, error)

	// IsEphemeralStorageEnabled returns true if ephemeral storage is supported, false otherwise.
	IsEphemeralStorageEnabled() bool

	// Queue enqueues a message: put the message to the tail of queue storage.
	// This function returns the enqueued QueueMessage with Id and QueueTimestamp fields filled.
	Queue(msg *QueueMessage) (*QueueMessage, error)

	// Requeue moves the enqueued message from ephemeral back to queue storage.
	//	- id: id of the message to be re-queued
	//	- silent: if true, message's requeue count and queue timestamp will not be updated; if false, message's requeue count is increased and queue timestamp is updated
	//
	// This function returns the enqueued QueueMessage with Id and QueueTimestamp fields filled.
	//
	// Notes:
	//	- message is put to head or tail of queue storage depending on queue implementation
	Requeue(id string, silent bool) (*QueueMessage, error)

	// Finish is called to signal that the message can now be removed from ephemeral storage.
	Finish(id string) error

	// Take dequeues a message: move a message from the head of queue storage to ephemeral storage and return the message.
	// Nil is returned if queue storage is empty.
	Take() (*QueueMessage, error)

	// OrphanMessages returns all messages that have been staying in ephemeral storage for more than a specific number of seconds.
	//	- numSeconds: messages older than <numSeconds> will be returned
	//	- numMessages: limit number of returned messages, value less than or equal to zero means 'no limit'
	//
	// Note: order of returned messages depends on queue implementation
	OrphanMessages(numSeconds, numMessages int) ([]*QueueMessage, error)

	// QueueSize returns number messages currently in queue storage.
	QueueSize() (int, error)

	// EphemeralSize returns number messages currently in ephemeral storage.
	EphemeralSize() (int, error)
}
