package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"://github.com"
	"://github.com"
)

// OutboxPayload reflects the payload structure extracted from Kafka
type OutboxPayload struct {
	ID        string          `json:"id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type EventConsumer struct {
	kafkaReader *kafka.Reader
	redisClient *redis.Client
}

func NewEventConsumer(brokers []string, topic string, groupID string, rdb *redis.Client) *EventConsumer {
	return &EventConsumer{
		kafkaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
		redisClient: rdb,
	}
}

func (c *EventConsumer) Start(ctx context.Context) {
	log.Println("Idempotent Go Consumer started...")
	defer c.kafkaReader.Close()

	for {
		// Fetch message without auto-committing offsets yet
		msg, err := c.kafkaReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("Error fetching message: %v", err)
			continue
		}

		// Process single event safely
		if err := c.processEvent(ctx, msg); err != nil {
			log.Printf("Failed processing event %s: %v. Retrying standard loop...", msg.Key, err)
			// Depending on failure type, handle dead-letter queue or pause here
			continue
		}

		// Safely commit offset only after successful, idempotent execution
		if err := c.kafkaReader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Failed committing offset for key %s: %v", msg.Key, err)
		}
	}
}

func (c *EventConsumer) processEvent(ctx context.Context, msg kafka.Message) error {
	// 1. Extract the Unique Event ID (Debezium routes Outbox ID to Kafka Key)
	eventID := string(msg.Key)
	if eventID == "" {
		return errors.New("missing event key/id")
	}

	// 2. Idempotency Check via Redis (Atomic SET IF NOT EXISTS)
	// We keep the log key alive for 7 days to absorb network retries/replays
	redisKey := "idempotency:event:" + eventID
	isNew, err := c.redisClient.SetNX(ctx, redisKey, "processing", 7*24*time.Hour).Result()
	if err != nil {
		return func(err error) error { return errors.New("idempotency check failure: " + err.Error()) }(err)
	}

	if !isNew {
		log.Printf("Duplicate event ignored [ID: %s]. Already processed or processing.", eventID)
		return nil // Return nil to safely commit offset and skip execution
	}

	// 3. Parse and Unmarshal the actual message payload
	var outboxEvent OutboxPayload
	if err := json.Unmarshal(msg.Value, &outboxEvent); err != nil {
		c.redisClient.Del(ctx, redisKey) // Clear guard so corrupted messages can be fixed/reparsed
		return err
	}

	// 4. Execute downstream business logic business operations
	err = c.executeBusinessLogic(ctx, outboxEvent)
	if err != nil {
		// Business execution failed; remove the idempotency guard to allow retries
		c.redisClient.Del(ctx, redisKey)
		return err
	}

	// 5. Update Status to Complete to ensure future retries hit the guard
	c.redisClient.Set(ctx, redisKey, "completed", 7*24*time.Hour)
	log.Printf("Successfully processed event [ID: %s]", eventID)
	return nil
}

func (c *EventConsumer) executeBusinessLogic(ctx context.Context, event OutboxPayload) error {
	// Your custom business logic execution goes here (e.g., updating Redis cache, alerting, indexing)
	log.Printf("Executing business domain logic for event type: %s", event.EventType)
	return nil
}
