package singu

import (
	"container/list"
	"sync"
	"time"
)

// NewInmemQueue creates a new InmemQueue instance.
//	- name: queue's name
//	- queueCapacity: if zero or negative queue storage has unlimited capacity; otherwise number of messages can be stored in queue storage is capped by the specified number
//	- ephemeralCapacity: if zero or negative ephemeral storage has unlimited capacity; otherwise ephemeral storage is capped by the specified number
func NewInmemQueue(name string, queueCapacity int, ephemeralDisabled bool, ephemeralCapacity int) IQueue {
	queue := &InmemQueue{
		name:              name,
		queueCapacity:     queueCapacity,
		ephemeralCapacity: ephemeralCapacity,
		ephemeralDisabled: ephemeralDisabled,
	}
	queue.Init()
	return queue
}

// InmemQueue is in-memory queue implementation.
//	- If queue message's id is not set, this queue implementation will assign one. Otherwise, the pre-set message id is used.
type InmemQueue struct {
	name                             string // queue's name
	queueCapacity, ephemeralCapacity int    // queue storage and ephemeral storage capacity
	ephemeralDisabled                bool   // is ephemeral storage disabled?

	queueStorage     *list.List               // queue storage implemented as a linked list
	ephemeralStorage map[string]*QueueMessage // ephemeral storage implemented as a map
	inited           bool                     // has this queue instance been initialized
	lock             sync.Mutex               // lock to avoid race condition
}

// Init initializes the queue instance
func (q *InmemQueue) Init() error {
	if !q.inited {
		if q.ephemeralDisabled || q.ephemeralCapacity < 0 {
			q.ephemeralCapacity = SizeNotSupported
		}
		if q.queueCapacity < 0 {
			q.queueCapacity = SizeNotSupported
		}

		q.queueStorage = list.New()
		if !q.ephemeralDisabled {
			q.ephemeralStorage = make(map[string]*QueueMessage)
		}
		q.inited = true
	}
	return nil
}

func (q *InmemQueue) ensureInit() error {
	if !q.inited {
		return q.Init()
	}
	return nil
}

// Destroy cleans up the queue instance
func (q *InmemQueue) Destroy() {
	if q.queueStorage != nil {
		q.queueStorage = nil
	}
	if q.ephemeralStorage != nil {
		q.ephemeralStorage = nil
	}
	q.inited = false
}

// Name implements IQueue.Name
func (q *InmemQueue) Name() string {
	return q.name
}

// QueueStorageCapacity implements IQueue.QueueStorageCapacity
func (q *InmemQueue) QueueStorageCapacity() (int, error) {
	return q.queueCapacity, nil
}

// EphemeralStorageCapacity implements IQueue.EphemeralStorageCapacity
func (q *InmemQueue) EphemeralStorageCapacity() (int, error) {
	return q.ephemeralCapacity, nil
}

// IsEphemeralStorageEnabled implements IQueue.IsEphemeralStorageEnabled
func (q *InmemQueue) IsEphemeralStorageEnabled() bool {
	return !q.ephemeralDisabled
}

// Queue implements IQueue.Queue
func (q *InmemQueue) Queue(msg *QueueMessage) (*QueueMessage, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if q.queueCapacity > 0 && q.queueStorage.Len() >= q.queueCapacity {
		return nil, ErrorQueueIsFull
	}

	clone := CloneQueueMessage(*msg)
	if clone.Id == "" {
		clone.Id = UniqueId()
	}
	clone.QueueTimestamp = time.Now()
	clone.TakenTimestamp = time.Time{}
	clone.NumRequeues = 0
	q.queueStorage.PushBack(clone)
	return &clone, nil
}

// Requeue implements IQueue.Requeue
func (q *InmemQueue) Requeue(id string, silent bool) (*QueueMessage, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if q.ephemeralDisabled {
		return nil, ErrorOperationNotSupported
	}
	if msg, ok := q.ephemeralStorage[id]; ok {
		msg.TakenTimestamp = time.Time{}
		if !silent {
			msg.QueueTimestamp = time.Now()
			msg.NumRequeues++
		}
		q.queueStorage.PushBack(*msg)
		delete(q.ephemeralStorage, id)
		clone := CloneQueueMessage(*msg)
		return &clone, nil
	}
	return nil, nil
}

// Finish implements IQueue.Finish
func (q *InmemQueue) Finish(id string) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	if err := q.ensureInit(); err != nil {
		return err
	}
	delete(q.ephemeralStorage, id)
	return nil
}

// Take implements IQueue.Take
func (q *InmemQueue) Take() (*QueueMessage, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if !q.ephemeralDisabled && q.ephemeralCapacity > 0 && len(q.ephemeralStorage) >= q.ephemeralCapacity {
		return nil, ErrorEphemeralIsFull
	}
	if el := q.queueStorage.Front(); el != nil {
		defer q.queueStorage.Remove(el)
		switch el.Value.(type) {
		case *QueueMessage:
			msg1 := CloneQueueMessage(*el.Value.(*QueueMessage))
			msg1.TakenTimestamp = time.Now()
			if !q.ephemeralDisabled {
				msg2 := CloneQueueMessage(msg1)
				q.ephemeralStorage[msg2.Id] = &msg2
			}
			return &msg1, nil
		case QueueMessage:
			msg1 := CloneQueueMessage(el.Value.(QueueMessage))
			msg1.TakenTimestamp = time.Now()
			if !q.ephemeralDisabled {
				msg2 := CloneQueueMessage(msg1)
				q.ephemeralStorage[msg2.Id] = &msg2
			}
			return &msg1, nil
		default:
			// TODO raise error?
		}
	}
	return nil, nil
}

// OrphanMessages implements IQueue.OrphanMessages
func (q *InmemQueue) OrphanMessages(numSeconds, numMessages int) ([]*QueueMessage, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if q.ephemeralDisabled {
		return nil, nil
	}
	result := make([]*QueueMessage, 0)
	now := time.Now()
	counter := 0
	for _, msg := range q.ephemeralStorage {
		if msg.TakenTimestamp.Unix()+int64(numSeconds) < now.Unix() {
			counter++
			clone := CloneQueueMessage(*msg)
			result = append(result, &clone)
			if numMessages > 0 && counter >= numMessages {
				break
			}
		}
	}
	return result, nil
}

// QueueSize implement IQueue.QueueSize
func (q *InmemQueue) QueueSize() (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.queueStorage == nil {
		return 0, nil
	}
	return q.queueStorage.Len(), nil
}

// EphemeralSize implements IQueue.EphemeralSize
func (q *InmemQueue) EphemeralSize() (int, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.ephemeralDisabled {
		return SizeNotSupported, nil
	}
	return len(q.ephemeralStorage), nil
}
