package boltqueue

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	topic := "foo"

	// Enqueue 50 messages.
	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Len(topic, p)
		if err != nil {
			t.Error(err)
		}
		if s != 10 {
			t.Errorf("Expected queue size 10 for priority %d. Got: %d", p, s)
		}
	}
}

func TestDequeue(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	topic := "bar"

	// Put them in in reverse priority order.
	for p := 5; p >= 1; p-- {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			mStrComp := fmt.Sprintf("test message %d-%d", p, n)
			m, err := testPQueue.Dequeue(topic)
			if err != nil {
				t.Error("Error dequeueing:", err)
			}
			mStr := m.ToString()
			if mStr != mStrComp {
				t.Errorf("Expected message=%q got=%q", mStrComp, mStr)
			}
			if m.Priority() != p {
				t.Errorf("Expected priority: %d, got: %d", p, m.Priority())
			}
		}
	}
	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Len(topic, p)
		if err != nil {
			t.Error(err)
		}
		if s != 0 {
			t.Errorf("Expected queue size 0 for priority %d. Got: %d", p, s)
		}
	}
}

func TestRequeue(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	topic := "baz"

	for p := 1; p <= 5; p++ {
		err := testPQueue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d", p)))
		if err != nil {
			t.Error(err)
		}
	}
	mp1, err := testPQueue.Dequeue(topic)
	if err != nil {
		t.Error(err)
	}
	// Remove the priority 2 message.
	_, _ = testPQueue.Dequeue(topic)

	// Re-enqueue the message at priority 1.
	err = testPQueue.Requeue(topic, 1, mp1)
	if err != nil {
		t.Error(err)
	}

	// And it should be the first to emerge.
	mp1, err = testPQueue.Dequeue(topic)
	if err != nil {
		t.Error(err)
	}

	if mp1.ToString() != "test message 1" {
		t.Errorf("Expected=%q, got=%q", "test message 1", mp1.ToString())
	}
}

func TestScan(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	topic := "zoo"

	// Put them in in reverse priority order.
	for p := 5; p >= 1; p-- {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	distinct := map[string]struct{}{}

	testPQueue.Scan(topic, func(m *Message) {
		distinct[m.ToString()] = struct{}{}
	})

	if expected, actual := 50, len(distinct); actual != expected {
		t.Errorf("Expected len(distinct)=%v but actual=%v", expected, actual)
	}
}

func TestScanWithBreak(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	topic := "zoo"

	// Put them in in reverse priority order.
	for p := 5; p >= 1; p-- {
		for n := 1; n <= 10; n++ {
			err := testPQueue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
			if err != nil {
				t.Error(err)
			}
		}
	}

	distinct := map[string]struct{}{}

	testPQueue.ScanWithBreak(topic, func(m *Message) bool {
		distinct[m.ToString()] = struct{}{}
		if m.ToString() == "test message 4-5" {
			return false
		}
		return true
	})

	if expected, actual := 35, len(distinct); actual != expected {
		t.Errorf("Expected len(distinct)=%v but actual=%v", expected, actual)
	}
}

func TestGoroutines(t *testing.T) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	testPQueue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		t.Error(err)
	}
	defer testPQueue.Close()
	defer os.Remove(queueFile)

	topic := "bam"

	var wg sync.WaitGroup

	for g := 1; g <= 5; g++ {
		wg.Add(1)
		go func() {
			rand.Seed(time.Now().Unix())
			time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
			for p := 1; p <= 5; p++ {
				for n := 1; n <= 2; n++ {
					err := testPQueue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d", p)))
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Len(topic, p)
		if err != nil {
			t.Error(err)
		}
		if s != 10 {
			t.Errorf("Expected queue size 10 for priority: %d. Got: %d", p, s)
		}
	}

	for p := 1; p <= 5; p++ {
		for n := 1; n <= 10; n++ {
			mStrComp := fmt.Sprintf("test message %d", p)
			m, err := testPQueue.Dequeue(topic)
			if err != nil {
				t.Error("Error dequeueing:", err)
			}
			mStr := m.ToString()
			if mStr != mStrComp {
				t.Errorf("Expected message=%q got=%q", mStrComp, mStr)
			}
			if m.Priority() != p {
				t.Errorf("Expected priority: %d, got: %d", p, m.Priority())
			}
		}
	}
	for p := 1; p <= 5; p++ {
		s, err := testPQueue.Len(topic, p)
		if err != nil {
			t.Error(err)
		}
		if s != 0 {
			t.Errorf("Expected queue size 0 for priority %d. Got: %d", p, s)
		}
	}
}

func BenchmarkPQueue(b *testing.B) {
	queueFile := fmt.Sprintf("%d_test.db", time.Now().UnixNano())
	queue, err := NewPQueueFromFile(queueFile)
	if err != nil {
		b.Error(err)
	}

	defer os.Remove(queueFile)

	topic := "bench"

	for n := 0; n < b.N; n++ {
		for p := 1; p <= 5; p++ {
			queue.Enqueue(topic, p, NewMessage(fmt.Sprintf("test message %d-%d", p, n)))
		}
	}

	for n := 0; n < b.N; n++ {
		for p := 1; p <= 6; p++ {
			_, err := queue.Dequeue(topic)
			if err != nil {
				b.Error(err)
			}
		}
	}
	if err = queue.Close(); err != nil {
		b.Error(err)
	}
}
