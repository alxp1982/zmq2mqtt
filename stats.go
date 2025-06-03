package main

import (
	"sync"
	"time"
)

type Stats struct {
	mu sync.RWMutex

	// Connection status
	zmqConnected  bool
	mqttConnected bool

	// Message counters
	messagesReceived  int64
	messagesForwarded int64

	// Rate calculation
	messageRate     float64
	lastMessageTime time.Time

	// Rolling window for rate calculation
	messageWindow []time.Time
	windowSize    int
}

func NewStats(windowSize int) *Stats {
	return &Stats{
		windowSize:    windowSize,
		messageWindow: make([]time.Time, 0, windowSize),
	}
}

// Methods for updating stats
func (s *Stats) UpdateConnectionStatus(zmq, mqtt bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.zmqConnected = zmq
	s.mqttConnected = mqtt
}

func (s *Stats) IncrementMessages(received, forwarded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if received {
		s.messagesReceived++
	}
	if forwarded {
		s.messagesForwarded++
	}

	// Update message window for rate calculation
	s.messageWindow = append(s.messageWindow, now)
	if len(s.messageWindow) > s.windowSize {
		s.messageWindow = s.messageWindow[1:]
	}

	s.lastMessageTime = now
	s.calculateRate()
}

func (s *Stats) calculateRate() {
	if len(s.messageWindow) < 2 {
		s.messageRate = 0
		return
	}

	duration := s.messageWindow[len(s.messageWindow)-1].Sub(s.messageWindow[0])
	if duration.Seconds() > 0 {
		s.messageRate = float64(len(s.messageWindow)) / duration.Seconds()
	}
}

// Methods for reading stats
func (s *Stats) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"zmq_connected":      s.zmqConnected,
		"mqtt_connected":     s.mqttConnected,
		"messages_received":  s.messagesReceived,
		"messages_forwarded": s.messagesForwarded,
		"message_rate":       s.messageRate,
		"last_message":       s.lastMessageTime,
	}
}
