package boltqueue

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/coreos/bbolt"
)

// TODO: Interfacification of messages

var foundItem = errors.New("item found")

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

// Enqueue adds a message to the queue
func (b *PQueue) Enqueue(topic string, priority int, messages ...*Message) error {
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
		for _, message := range messages {
			if message.Key == 0 {
				id, err := pb.NextSequence()
				if err != nil {
					return err
				}
				message.Key = id
			}
			key := make([]byte, binary.MaxVarintLen64)
			binary.BigEndian.PutUint64(key, message.Key)
			if err = pb.Put(key, message.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Requeue adds a message back into the queue, keeping its precedence.
// If added at the same priority, it should be among the first to dequeue.
// If added at a different priority, it will dequeue before newer messages
// of that priority.
func (b *PQueue) Requeue(topic string, priority int, messages ...*Message) error {
	for _, message := range messages {
		if message.Key == 0 {
			return fmt.Errorf("Cannot requeue new message")
		}
	}
	return b.Enqueue(topic, priority, messages...)
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
			kb, v := cur.First() // Should not be empty by definition since we just checked.

			priority, _ := binary.Uvarint(bp)

			m = &Message{
				Key:      binary.BigEndian.Uint64(kb),
				Value:    cloneBytes(v),
				priority: int(priority),
			}

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
