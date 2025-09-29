package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type ConsumerInterface interface {
	Consumer(ctx context.Context, handler func(kafka.Message)) error
}
