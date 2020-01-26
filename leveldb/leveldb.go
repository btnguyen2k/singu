// Package leveldb contains queue implementation using LevelDB as backend storage.
package leveldb

import (
	"encoding/json"
	"github.com/btnguyen2k/singu"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"strings"
	"sync"
	"time"
)

// NewLeveldbQueue creates a new LeveldbQueue instance.
//	- name: queue's name
//	- dataPath: root directory to store LevelDB data, actual data is stored in <name> sub-directory
//	- queueCapacity: if zero or negative queue storage has unlimited capacity; otherwise number of messages can be stored in queue storage is capped by the specified number
//	- ephemeralCapacity: if zero or negative ephemeral storage has unlimited capacity; otherwise ephemeral storage is capped by the specified number
func NewLeveldbQueue(name, dataPath string, queueCapacity int, ephemeralDisabled bool, ephemeralCapacity int) singu.IQueue {
	queue := &LeveldbQueue{
		name:              name,
		dataPath:          dataPath,
		queueCapacity:     queueCapacity,
		ephemeralCapacity: ephemeralCapacity,
		ephemeralDisabled: ephemeralDisabled,
	}
	queue.Init()
	return queue
}

const (
	prefixQueue     = "queue-"
	prefixEphemeral = "ephemeral-"
	keyLastTakenId  = "last-taken-id"
)

// LeveldbQueue is LevelDB queue implementation.
//	- This queue implementation does not use the pre-set message it. It always assigns assign new id for every enqueued message.
type LeveldbQueue struct {
	name                             string // queue's name
	queueCapacity, ephemeralCapacity int    // queue storage and ephemeral storage capacity
	ephemeralDisabled                bool   // is ephemeral storage disabled?
	dataPath                         string // root directory to store LevelDB data, actual data is stored in <name> sub-directory

	lastTakenId string
	db          *leveldb.DB // LevelDB instance
	inited      bool        // has this queue instance been initialized
	lockInit    sync.Mutex  // lock to avoid race condition
	lockTake    sync.Mutex  // lock to avoid race condition
}

// Init initializes the queue instance
func (q *LeveldbQueue) Init() error {
	if !q.inited {
		if q.ephemeralDisabled || q.ephemeralCapacity < 0 {
			q.ephemeralCapacity = singu.SizeNotSupported
		}
		if q.queueCapacity < 0 {
			q.queueCapacity = singu.SizeNotSupported
		}
		q.dataPath = strings.TrimSuffix(q.dataPath, "/")
		if db, err := leveldb.OpenFile(q.dataPath+"/"+q.name, nil); err != nil {
			return err
		} else {
			q.db = db
		}
		if lastTakenId, err := q.db.Get([]byte(keyLastTakenId), nil); err != nil {
			q.lastTakenId = string(lastTakenId)
		}
		q.inited = true
	}
	return nil
}

func (q *LeveldbQueue) ensureInit() error {
	if !q.inited {
		q.lockInit.Lock()
		defer q.lockInit.Unlock()
		return q.Init()
	}
	return nil
}

// Destroy cleans up the queue instance
func (q *LeveldbQueue) Destroy() {
	if q.db != nil {
		// q.db.Put([]byte(keyLastTakenId), []byte(q.lastTakenId), nil)
		q.db.Close()
		q.db = nil
	}
	q.inited = false
}

// Name implements IQueue.Name
func (q *LeveldbQueue) Name() string {
	return q.name
}

// QueueStorageCapacity implements IQueue.QueueStorageCapacity
func (q *LeveldbQueue) QueueStorageCapacity() (int, error) {
	return q.queueCapacity, nil
}

// EphemeralStorageCapacity implements IQueue.EphemeralStorageCapacity
func (q *LeveldbQueue) EphemeralStorageCapacity() (int, error) {
	return q.ephemeralCapacity, nil
}

// IsEphemeralStorageEnabled implements IQueue.IsEphemeralStorageEnabled
func (q *LeveldbQueue) IsEphemeralStorageEnabled() bool {
	return !q.ephemeralDisabled
}

// Queue implements IQueue.Queue
func (q *LeveldbQueue) Queue(msg *singu.QueueMessage) (*singu.QueueMessage, error) {
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if q.queueCapacity > 0 {
		if queueSize, err := q.countRangePrefix(prefixQueue); err != nil {
			return nil, err
		} else if queueSize >= q.queueCapacity {
			return nil, singu.ErrorQueueIsFull
		}
	}

	clone := singu.CloneQueueMessage(*msg)
	clone.Id = singu.UniqueId()
	clone.QueueTimestamp = time.Now()
	clone.TakenTimestamp = time.Time{}
	clone.NumRequeues = 0
	value, _ := json.Marshal(clone)
	return &clone, q.db.Put([]byte(prefixQueue+clone.Id), value, nil)
}

// Requeue implements IQueue.Requeue
func (q *LeveldbQueue) Requeue(id string, silent bool) (*singu.QueueMessage, error) {
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if q.ephemeralDisabled {
		return nil, singu.ErrorOperationNotSupported
	}
	if value, err := q.db.Get([]byte(prefixEphemeral+id), nil); err != nil {
		if err != leveldb.ErrNotFound {
			return nil, err
		}
	} else {
		batch := new(leveldb.Batch)
		batch.Delete([]byte(prefixEphemeral + id))
		var msg singu.QueueMessage
		if err := json.Unmarshal(value, &msg); err != nil {
			return nil, err
		}
		msg.Id = singu.UniqueId()
		msg.TakenTimestamp = time.Time{}
		if !silent {
			msg.QueueTimestamp = time.Now()
			msg.NumRequeues++
		}
		js, _ := json.Marshal(msg)
		batch.Put([]byte(prefixQueue+msg.Id), js)
		return &msg, q.db.Write(batch, nil)
	}
	return nil, nil
}

// Finish implements IQueue.Finish
func (q *LeveldbQueue) Finish(id string) error {
	if err := q.ensureInit(); err != nil {
		return err
	}
	return q.db.Delete([]byte(prefixEphemeral+id), nil)
}

// Take implements IQueue.Take
func (q *LeveldbQueue) Take() (*singu.QueueMessage, error) {
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	if !q.ephemeralDisabled && q.ephemeralCapacity > 0 {
		if ephemeralSize, err := q.countRangePrefix(prefixEphemeral); err != nil {
			return nil, err
		} else if ephemeralSize >= q.ephemeralCapacity {
			return nil, singu.ErrorEphemeralIsFull
		}
	}
	q.lockTake.Lock()
	defer q.lockTake.Unlock()
	iter := q.db.NewIterator(util.BytesPrefix([]byte(prefixQueue)), nil)
	defer iter.Release()
	if iter.Seek([]byte(q.lastTakenId)) {
		key := iter.Key()
		value := iter.Value()
		var msg singu.QueueMessage
		if err := json.Unmarshal(value, &msg); err != nil {
			return nil, err
		}
		msg.TakenTimestamp = time.Now()
		batch := new(leveldb.Batch)
		batch.Delete(key)
		if !q.ephemeralDisabled {
			js, _ := json.Marshal(msg)
			batch.Put([]byte(prefixEphemeral+msg.Id), js)
		}
		batch.Put([]byte(keyLastTakenId), key)
		if err := q.db.Write(batch, nil); err == nil {
			q.lastTakenId = string(key)
			return &msg, nil
		} else {
			return &msg, err
		}
	}
	return nil, nil
}

// OrphanMessages implements IQueue.OrphanMessages
func (q *LeveldbQueue) OrphanMessages(numSeconds int64) ([]*singu.QueueMessage, error) {
	if err := q.ensureInit(); err != nil {
		return nil, err
	}
	iter := q.db.NewIterator(util.BytesPrefix([]byte(prefixEphemeral)), nil)
	defer iter.Release()
	result := make([]*singu.QueueMessage, 0)
	now := time.Now()
	for iter.Next() {
		value := iter.Value()
		var msg singu.QueueMessage
		if err := json.Unmarshal(value, &msg); err == nil && msg.TakenTimestamp.Unix()+numSeconds < now.Unix() {
			result = append(result, &msg)
		}
	}
	return result, nil
}

func (q *LeveldbQueue) countRangePrefix(prefix string) (int, error) {
	iter := q.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()
	count := 0
	for iter.Next() {
		count++
	}
	return count, nil
}

// QueueSize implements IQueue.QueueSize
func (q *LeveldbQueue) QueueSize() (int, error) {
	if err := q.ensureInit(); err != nil {
		return 0, err
	}
	return q.countRangePrefix(prefixQueue)
}

// EphemeralSize implements IQueue.EphemeralSize
func (q *LeveldbQueue) EphemeralSize() (int, error) {
	if err := q.ensureInit(); err != nil {
		return 0, err
	}
	return q.countRangePrefix(prefixEphemeral)
}
