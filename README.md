# bboltqueue[![GoDoc](https://godoc.org/jaytaylor.com/bboltqueue?status.svg)](https://godoc.org/jaytaylor.com/bboltqueue)

[![Documentation](https://godoc.org/github.com/jaytaylor/bboltqueue?status.svg)](https://godoc.org/github.com/jaytaylor/bboltqueue)
[![Build Status](https://travis-ci.org/jaytaylor/bboltqueue.svg?branch=master)](https://travis-ci.org/jaytaylor/bboltqueue)
[![Report Card](https://goreportcard.com/badge/github.com/jaytaylor/bboltqueue)](https://goreportcard.com/report/github.com/jaytaylor/bboltqueue)


```go
import "jaytaylor.com/bboltqueue"
```

Package boltqueue provides a persistent queue or priority queue based on [boltdb](https://github.com/coreos/bbolt).

Based on [boltqueue](https://github.com/flowchartsman/boltqueue).


### Priority Queue

boltqueue's PQueue type represents a priority queue. Messages may be inserted
into the queue at a numeric priority between 0(highest) and 255(lowest).
Messages are dequeued following priority order, then time ordering, with the
oldest messages of the highest priority emerging first.

## Usage

#### type Message

```go
type Message struct {
}
```

Message represents a message in the priority queue

#### func  NewMessage

```go
func NewMessage(value string) *Message
```
NewMessage generates a new priority queue message

#### func (*Message) Priority

```go
func (m *Message) Priority() int
```
Priority returns the priority the message had in the queue in the range of 0-255
or -1 if the message is new.

#### func (*Message) ToString

```go
func (m *Message) ToString() string
```
ToString outputs the string representation of the message's value

#### type PQueue

```go
type PQueue struct {
}
```

PQueue is a priority queue backed by a Bolt database on disk

#### func NewPQueue

```go
func NewPQueue(db *bolt.DB) (*PQueue, error)
```
NewPQueue uses an already open `*bolt.DB`

#### func NewPQueueFromFile

```go
func NewPQueueFromFile(filename string) (*PQueue, error)
```
NewPQueueFromFile loads or creates a new PQueue with the given filename

#### func (*PQueue) Close

```go
func (b *PQueue) Close() error
```
Close closes the queue and releases all resources

#### func (*PQueue) Dequeue

```go
func (b *PQueue) Dequeue(topic string) (*Message, error)
```
Dequeue removes the oldest, highest priority message from the queue of the named
topic and returns it

#### func (*PQueue) Enqueue

```go
func (b *PQueue) Enqueue(topic string, priority int, message *Message) error
```
Enqueue adds a message to the queue filed under the specified topic

#### func (*PQueue) Requeue

```go
func (b *PQueue) Requeue(topic string, priority int, message *Message) error
```
Requeue adds a message back into the topic queue, keeping its precedence. If
added at the same priority, it should be among the first to dequeue. If added at
a different priority, it will dequeue before newer messages of that priority.

#### func (*PQueue) Size

```go
func (b *PQueue) Size(topic string, priority int) (int, error)
```
Size returns the number of entries of a given priority from 1 to 5
