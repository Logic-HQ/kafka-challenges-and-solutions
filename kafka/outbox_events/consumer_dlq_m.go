package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"://github.com"
	"://github.com/promhttp"
	"://github.com"
	"://github.com"
)

```
Extended version of Idempotent Go Consumer with DLQ.
( NOT RUN ON Production as this is examole! ) "Production-oriented" Consumer Pattern: Dead Letter Queue (DLQ) & Metrics
Please, read the docs:
https://github.com/Logic-HQ/kafka-challenges-and-solutions/wiki/Docker-Compose-environment-EOP-fpr-Go-app
```


// Prometheus Metrics Declarations
var (
	eventsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_events_processed_total",
			Help: "Total number of events processed successfully.",
		},
		[]string{"event_type"},
	)
	eventsFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_events_failed_total",
			Help: "Total number of events routed to the DLQ.",
		},
		[]string{"event_type", "reason"},
	)
	eventsDuplicate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "consumer_events_duplicate_total",
			Help: "Total number of duplicate events caught by Redis guard.",
		},
	)
)

func init() {
	// Register application metrics to the global Prometheus registry
	prometheus.MustRegister(eventsProcessed)
	prometheus.MustRegister(eventsFailed)
	prometheus.MustRegister(eventsDuplicate)
}

type OutboxPayload struct {
	ID        string          `json:"id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type DLQEnvelope struct {
	OriginalKey     string    `json:"original_key"`
	OriginalPayload string    `json:"original_payload"`
	ErrorReason     string    `json:"error_reason"`
	FailedAt        time.Time `json:"failed_at"`
}

type ManagedConsumer struct {
	kafkaReader *kafka.Reader
	dlqWriter   *kafka.Writer
	redisClient *redis.Client
}

func NewManagedConsumer(brokers []string, topic, groupID string, rdb *redis.Client) *ManagedConsumer {
	return &ManagedConsumer{
		kafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		}),
		// Separate writer specifically dedicated to pushing to the DLQ topic
		dlqWriter: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic + "-dlq", // e.g., account-events-dlq
			Balancer: &kafka.LeastBytes{},
		},
		redisClient: rdb,
	}
}

func (c *ManagedConsumer) Start(ctx context.Context) {
	log.Println("Production Event Consumer processing initialized...")
	defer c.kafkaReader.Close()
	defer c.dlqWriter.Close()

	for {
		msg, err := c.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("Poll offset fetch failure: %v", err)
			continue
		}

		// Ensure errors are handled internally so the loop NEVER crashes or blocks indefinitely
		if err := c.processEventWithDLQ(ctx, msg); err != nil {
			log.Printf("Critical: Message key %s fell through to DLQ failure state: %v", msg.Key, err)
		}

		// Always commit the message. Poison pills are safely pushed to DLQ, clean records are executed.
		if err := c.kafkaReader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Offset commit fatal tracking exception: %v", err)
		}
	}
}

func (c *ManagedConsumer) processEventWithDLQ(ctx context.Context, msg kafka.Message) error {
	eventID := string(msg.Key)
	if eventID == "" {
		c.routeToDLQ(ctx, msg, "Missing application identity record key")
		return errors.New("empty event payload identity")
	}

	redisKey := "idempotency:event:" + eventID
	isNew, err := c.redisClient.SetNX(ctx, redisKey, "processing", 7*24*time.Hour).Result()
	if err != nil {
		eventsFailed.WithLabelValues("unknown", "redis_failure").Inc()
		return err
	}

	if !isNew {
		eventsDuplicate.Inc()
		log.Printf("[Idempotency Block] Event skipped: %s", eventID)
		return nil
	}

	var outboxEvent OutboxPayload
	if err := json.Unmarshal(msg.Value, &outboxEvent); err != nil {
		// Poison pill discovered (malformed JSON format). Route to DLQ!
		c.routeToDLQ(ctx, msg, "Malformed JSON format: "+err.Error())
		eventsFailed.WithLabelValues("unparseable", "json_error").Inc()
		
		// Set to "completed" in Redis so we don't process this corrupt message ID again
		c.redisClient.Set(ctx, redisKey, "completed", 7*24*time.Hour)
		return err
	}

	// Execute actual application domain logic
	if err := c.executeDomainLogic(ctx, outboxEvent); err != nil {
		// Business execution failed; clear the idempotency check to allow future retries
		c.redisClient.Del(ctx, redisKey)
		eventsFailed.WithLabelValues(outboxEvent.EventType, "business_logic_error").Inc()
		return err
	}

	// Update key status to completed on smooth operational execution
	c.redisClient.Set(ctx, redisKey, "completed", 7*24*time.Hour)
	eventsProcessed.WithLabelValues(outboxEvent.EventType).Inc()
	return nil
}

func (c *ManagedConsumer) routeToDLQ(ctx context.Context, originalMsg kafka.Message, reason string) {
	envelope := DLQEnvelope{
		OriginalKey:     string(originalMsg.Key),
		OriginalPayload: string(originalMsg.Value),
		ErrorReason:     reason,
		FailedAt:        time.Now(),
	}

	payloadBytes, _ := json.Marshal(envelope)

	err := c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   originalMsg.Key,
		Value: payloadBytes,
	})
	if err != nil {
		log.Printf("CRITICAL METRIC EXCEPTION: DLQ Pipeline execution write failed: %v", err)
	} else {
		log.Printf("Poison pill isolated. Forwarded to DLQ [Key: %s]. Reason: %s", originalMsg.Key, reason)
	}
}

func (c *ManagedConsumer) executeDomainLogic(ctx context.Context, event OutboxPayload) error {
	// Simulate an error for demonstration purposes if amount matches a test value
	if strings.Contains(string(event.Payload), "force_error") {
		return errors.New("simulated internal engine dependency rejection")
	}
	log.Printf("Event %s successfully ingested into system domain runtime.", event.ID)
	return nil
}

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	redisAddr := os.Getenv("REDIS_ADDR")

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	// Start internal HTTP endpoint specifically for exposing Prometheus scrapers
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Prometheus target scrape engine listening on :8080/metrics")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Metrics engine crash: %v", err)
		}
	}()

	consumer := NewManagedConsumer(brokers, "account-events", "payment-service-group", rdb)
	consumer.Start(context.Background())
}
