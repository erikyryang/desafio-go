// Package sqs integrates with AWS SQS (LocalStack locally): queue resolution,
// the wager-transactions consumer and the outbox publisher target.
package sqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Config for the SQS client.
type Config struct {
	Region          string
	Endpoint        string // LocalStack endpoint; empty uses the AWS default
	AccessKeyID     string
	SecretAccessKey string
}

// NewClient builds an SQS client. Credentials come from configuration (broker
// access is controlled by IAM/queue policies; see ARCHITECTURE.md).
func NewClient(ctx context.Context, cfg Config) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// Queues holds the resolved queue URLs.
type Queues struct {
	WagerTransactions string
	WagerDLQ          string
	WalletEvents      string
}

// ResolveQueues looks up queue URLs by name, failing fast when a queue is missing.
func ResolveQueues(ctx context.Context, client *sqs.Client, wager, dlq, events string) (Queues, error) {
	var q Queues
	var err error
	if q.WagerTransactions, err = queueURL(ctx, client, wager); err != nil {
		return q, err
	}
	if q.WagerDLQ, err = queueURL(ctx, client, dlq); err != nil {
		return q, err
	}
	if q.WalletEvents, err = queueURL(ctx, client, events); err != nil {
		return q, err
	}
	return q, nil
}

func queueURL(ctx context.Context, client *sqs.Client, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", fmt.Errorf("resolve queue %s: %w", name, err)
	}
	return aws.ToString(out.QueueUrl), nil
}

// ReadinessCheck verifies the broker by reading queue attributes.
type ReadinessCheck struct {
	Client   *sqs.Client
	QueueURL string
}

func (ReadinessCheck) Name() string { return "sqs" }

// Check implements httpapi.ReadinessCheck.
func (c ReadinessCheck) Check(ctx context.Context) error {
	_, err := c.Client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.QueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}
