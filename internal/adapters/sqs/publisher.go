package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/erikyryan/desafio-go/internal/app"
)

// EventPublisher sends outbox records to the wallet-events FIFO queue.
//
// Routing contract: MessageGroupId = aggregateId (ordering per wallet /
// transaction), MessageDeduplicationId = eventId (5-minute broker dedup on
// republication), message attributes eventType, aggregateType and
// correlationId for consumer-side filtering. Body = the envelope JSON.
type EventPublisher struct {
	client   *sqs.Client
	queueURL string
}

// NewEventPublisher wires the target queue.
func NewEventPublisher(client *sqs.Client, queueURL string) *EventPublisher {
	return &EventPublisher{client: client, queueURL: queueURL}
}

// Publish sends one event.
func (p *EventPublisher) Publish(ctx context.Context, rec app.OutboxRecord) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(rec.Payload)),
		MessageGroupId:         aws.String(rec.AggregateID.String()),
		MessageDeduplicationId: aws.String(rec.EventID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType":     {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
			"aggregateType": {DataType: aws.String("String"), StringValue: aws.String(rec.AggregateType)},
			"correlationId": {DataType: aws.String("String"), StringValue: aws.String(orDash(rec.CorrelationID))},
		},
	})
	if err != nil {
		return fmt.Errorf("publish %s %s: %w", rec.EventType, rec.EventID, err)
	}
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
