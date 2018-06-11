package boltqueue

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/coreos/bbolt"
)

// TODO: Interfacification of messages

var foundItem = errors.New("item found")

// aKey singleton for assigning keys to messages
var aKey = new(atomicKey)

// PQueue is a priority queue backed by a Bolt database on disk
type PQueue struct {
	conn *bolt.DB
}

// NewPQueue consumes an existing opened *bold.DB.
func NewPQueue(db *bolt.DB) *PQueue {
	b := &PQueue{
		conn: db,
	}
	return b
}

// NewPQueueFromFile loads or creates a new PQueue with the given filename
func NewPQueueFromFile(filename string) (*PQueue, error) {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return nil, err
	}

	b := &PQueue{
		conn: db,
	}
	return b, nil
}

func (b *PQueue) enqueueMessage(topic string, priority int, key []byte, message *Message) error {
	if priority < 0 || priority > 255 {
		return fmt.Errorf("Invalid priority %d on Enqueue", priority)
	}
	p := append([]byte(topic), byte(uint8(priority)))
	return b.conn.Update(func(tx *bolt.Tx) error {
		// Get bucket for this priority level
		pb, err := tx.CreateBucketIfNotExists(p)
		if err != nil {
			return err
		}
		err = pb.Put(key, message.value)
		if err != nil {
			return err
		}
		return nil
	})
}

// Enqueue adds a message to the queue
func (b *PQueue) Enqueue(topic string, priority int, message *Message) error {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, aKey.Get())
	return b.enqueueMessage(topic, priority, k, message)
}

// Requeue adds a message back into the queue, keeping its precedence.
// If added at the same priority, it should be among the first to dequeue.
// If added at a different priority, it will dequeue before newer messages
// of that priority.
func (b *PQueue) Requeue(topic string, priority int, message *Message) error {
	if message.key == nil {
		return fmt.Errorf("Cannot requeue new message")
	}
	return b.enqueueMessage(topic, priority, message.key, message)
}

// Dequeue removes the oldest, highest priority message from the queue and
// returns it
func (b *PQueue) Dequeue(topic string) (*Message, error) {
	var m *Message
	err := b.conn.Update(func(tx *bolt.Tx) error {
		err := tx.ForEach(func(bname []byte, bucket *bolt.Bucket) error {
			// Only operate on buckets which match topic.
			btopic := string(bname)[0 : len(bname)-1]
			if btopic != topic {
				return nil
			}
			bp := bname[len(bname)-1 : len(bname)]

			if bucket.Stats().KeyN == 0 { //empty bucket
				return nil
			}

			cur := bucket.Cursor()
			k, v := cur.First() //Should not be empty by definition
			priority, _ := binary.Uvarint(bp)
			m = &Message{priority: int(priority), key: cloneBytes(k), value: cloneBytes(v)}

			// Remove message
			if err := cur.Delete(); err != nil {
				return err
			}
			return foundItem //to stop the iteration
		})
		if err != nil && err != foundItem {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Size returns the number of entries of a given priority from 1 to 5
func (b *PQueue) Size(topic string, priority int) (int, error) {
	if priority < 0 || priority > 255 {
		return 0, fmt.Errorf("Invalid priority %d for Size()", priority)
	}
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	btopic := append([]byte(topic), byte(uint8(priority)))
	bucket := tx.Bucket(btopic)
	if bucket == nil {
		return 0, nil
	}
	count := bucket.Stats().KeyN
	tx.Rollback()
	return count, nil
}

// Close closes the queue and releases all resources
func (b *PQueue) Close() error {
	err := b.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// taken from boltDB. Avoids corruption when re-queueing
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
