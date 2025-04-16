package pubsub

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
	"os"
)

// Create a new topic on RabbitMQ. This requires the environment
// variable `RABBIT_SERVER_URL` to be set.
func NewRabbitTopic(ctx context.Context, topic string) (*pubsub.Topic, error) {
	return pubsub.OpenTopic(ctx, fmt.Sprintf("rabbit://%s", topic))
}

func NewRabbitSubscription(ctx context.Context, topic, subscription string) (*pubsub.Subscription, func(), error) {
	if subscription == "" || topic == "" {
		return nil, nil, fmt.Errorf("subscription and topic must not be empty")
	}

	rabbitURL := os.Getenv("RABBIT_SERVER_URL")
	if rabbitURL == "" {
		return nil, nil, fmt.Errorf("RABBIT_SERVER_URL must be set (e.g., amqp://guest:guest@localhost:5672/)")
	}

	logrus.Debugf("Connecting to RabbitMQ: %s", rabbitURL)
	conn, err := amqp091.Dial(rabbitURL)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ at %s: %w", rabbitURL, err)
	}

	logrus.Debug("Creating RabbitMQ channel")
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open channel: %w", err)
	}

	logrus.Debugf("Declaring fanout exchange: %s", topic)
	err = ch.ExchangeDeclare(
		topic,    // exchange name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to declare exchange %s: %w", topic, err)
	}

	logrus.Debugf("Declaring queue: %s", subscription)
	_, err = ch.QueueDeclare(
		subscription,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to declare queue %s: %w", subscription, err)
	}

	logrus.Debugf("Binding queue %s to exchange %s", subscription, topic)
	err = ch.QueueBind(
		subscription,
		"",
		topic,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("failed to bind queue %s to exchange %s: %w", subscription, topic, err)
	}

	logrus.Debug("Closing RabbitMQ channel")
	err = ch.Close()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to close channel: %w", err)
	}

	subURL := fmt.Sprintf("rabbit://%s", subscription)
	logrus.Debugf("Opening RabbitMQ subscription: %s", subURL)
	sub, err := pubsub.OpenSubscription(ctx, subURL)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to open subscription for queue %s: %w", subscription, err)
	}
	
	cleanup := func() {
		logrus.Debug("Shutting down subscription")
		if err := sub.Shutdown(ctx); err != nil {
			logrus.Errorf("Error shutting down subscription: %v", err)
		}
		logrus.Debug("Closing RabbitMQ connection")
		if err := conn.Close(); err != nil {
			logrus.Errorf("Error closing RabbitMQ connection: %v", err)
		}
	}

	logrus.Infof("Successfully opened subscription for queue %s on exchange %s", subscription, topic)
	return sub, cleanup, nil
}
