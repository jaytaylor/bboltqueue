package boltqueue

// Message represents a message in the priority queue
type Message struct {
	Key      uint64
	Value    []byte
	priority int
}

// NewMessage generates a new priority queue message
func NewMessage(value string) *Message {
	m := &Message{
		Value:    []byte(value),
		priority: -1,
	}
	return m
}

// Priority returns the priority the message had in the queue in the range of
// 0-255 or -1 if the message is new.
func (m *Message) Priority() int {
	return m.priority
}

// ToString outputs the string representation of the message's value
func (m *Message) ToString() string {
	return string(m.Value)
}
