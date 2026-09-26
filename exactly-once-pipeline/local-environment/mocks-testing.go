package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"://github.com"
	"://github.com"
	"://github.com"
)

// MockIdempotencyStore defines mock expectations
type MockIdempotencyStore struct {
	mock.Mock
}

func (m *MockIdempotencyStore) SetNX(ctx context.Context, key string, value interface{}, exp time.Duration) (bool, error) {
	args := m.Called(ctx, key, value, exp)
	return args.Bool(0), args.Error(1)
}
func (m *MockIdempotencyStore) Del(ctx context.Context, key string) error {
	return m.Called(ctx, key).Error(0)
}
func (m *MockIdempotencyStore) Set(ctx context.Context, key string, value interface{}, exp time.Duration) error {
	return m.Called(ctx, key, value, exp).Error(0)
}

func TestProcessMessage_IdempotencyGuard(t *testing.T) {
	t.Run("Should process clean, new events successfully", func(t *testing.T) {
		mockStore := new(MockIdempotencyStore)
		ctx := context.Background()
		eventKey := "tx_unique_123"

		// Expectation: SetNX returns true (Key does not exist yet)
		mockStore.On("SetNX", ctx, "idempotency:avro:"+eventKey, "processing", 24*time.Hour).Return(true, nil)
		mockStore.On("Set", ctx, "idempotency:avro:"+eventKey, "completed", 24*time.Hour).Return(nil)

		// Create a standard mock Kafka payload (skipping schema wire formatting bytes for simpler mock validation logic)
		msg := kafka.Message{
			Key:   []byte(eventKey),
			Value: append([]byte{0, 0, 0, 0, 1}, []byte(`mock_avro_payload_bytes`)...), 
		}

		// (Assuming processMessage accepts your IdempotencyStore interface)
		err := processMessageWithStore(ctx, msg, mockStore)

		assert.Nil(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("Should skip duplicate incoming event drops cleanly", func(t *testing.T) {
		mockStore := new(MockIdempotencyStore)
		ctx := context.Background()
		eventKey := "tx_duplicate_456"

		// Expectation: SetNX returns false (Key already recorded in cache!)
		mockStore.On("SetNX", ctx, "idempotency:avro:"+eventKey, "processing", 24*time.Hour).Return(false, nil)

		msg := kafka.Message{
			Key: []byte(eventKey),
		}

		err := processMessageWithStore(ctx, msg, mockStore)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "event duplicate")
		mockStore.AssertExpectations(t)
	})
}

