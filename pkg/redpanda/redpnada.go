package redpanda

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"sync"
	"time"
)

type RedpandaSession struct {
	readers map[string]*kafka.Reader
	mutex   sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	brokers []string
	topic   string
	groupID string
	dialer  *kafka.Dialer
}

type RedpandaConfig struct {
	Brokers       []string
	Debug         string
	Username      string
	Password      string
	SASLMechanism string
	SASLProtocol  string
	GroupID       string
	EnableSSL     bool // Derived from SASLProtocol
}

type QueueConfig struct {
	Queue struct {
		Private []string `yaml:"private"`
		Public  []string `yaml:"public"`
	} `yaml:"queue"`
}

var (
	errShutdown = errors.New("session is shutting down")
)

// NewRedpandaSession initializes a Redpanda (Kafka) producer session.
func NewRedpandaSession(config RedpandaConfig, topic, groupID string) *RedpandaSession {
	ctx, cancel := context.WithCancel(context.Background())

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Enable SASL (if username and password are provided)
	if config.Username != "" && config.Password != "" {
		mechanism, err := scram.Mechanism(scram.SHA512, config.Username, config.Password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	// Enable TLS
	if config.EnableSSL {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true, // ⚠️ optionally make strict for production
		}
	}

	return &RedpandaSession{
		brokers: config.Brokers,
		topic:   topic,
		groupID: groupID,
		dialer:  dialer,
		readers: make(map[string]*kafka.Reader),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Stream continuously reads messages from a topic and passes them to a consumer function.
func (s *RedpandaSession) Stream(topic string, consumer func(kafka.Message)) {
	s.mutex.Lock()
	if _, exists := s.readers[topic]; exists {
		s.mutex.Unlock()
		log.Warn().Msgf("Already streaming topic: %s", topic)
		return
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.brokers,
		GroupID:     s.groupID,
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		Dialer:      s.dialer,
	})
	s.readers[topic] = reader
	s.mutex.Unlock()

	go func() {
		log.Info().Msgf("Started consuming Redpanda topic: %s", topic)
		for {
			select {
			case <-s.ctx.Done():
				log.Info().Msgf("Stopped consuming topic: %s", topic)
				return
			default:
				m, err := reader.ReadMessage(s.ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						log.Warn().Msgf("Redpanda consumer context closed for topic: %s", topic)
						return
					}
					log.Error().Err(err).Msgf("Redpanda Read failed on topic: %s", topic)
					time.Sleep(time.Second) // avoid tight retry loop
					continue
				}
				consumer(m)
			}
		}
	}()
}

// Close shuts down the session: all readers and the writer.
func (s *RedpandaSession) Close() error {
	log.Info().Msg("Closing Redpanda session")
	s.cancel()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	for topic, reader := range s.readers {
		if err := reader.Close(); err != nil {
			log.Error().Err(err).Msgf("Error closing reader for topic %s", topic)
		}
	}
	s.readers = nil

	return nil
}
