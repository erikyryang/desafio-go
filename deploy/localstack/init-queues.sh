#!/bin/bash
# Provisions the FIFO queues, the redrive policy and an access policy.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"

awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"1209600"}' >/dev/null

DLQ_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:wager-transactions-dlq.fifo"

# maxReceiveCount=5: a message that fails transiently five times (visibility
# timeout 30s each) is moved to the DLQ by the broker.
awslocal sqs create-queue --queue-name wager-transactions.fifo \
  --attributes "{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"VisibilityTimeout\":\"30\",\"MessageRetentionPeriod\":\"345600\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}" >/dev/null

# Destination of the transactional outbox.
awslocal sqs create-queue --queue-name wallet-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","MessageRetentionPeriod":"345600"}' >/dev/null

# Broker-side access control: only the wager service principal may consume
# and publish. LocalStack (community) does not enforce IAM; the policy documents
# the intended production configuration and is applied for fidelity.
for Q in wager-transactions.fifo wager-transactions-dlq.fifo wallet-events.fifo; do
  URL=$(awslocal sqs get-queue-url --queue-name "$Q" --query QueueUrl --output text)
  ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:${Q}"
  ATTRS=$(ARN="$ARN" ACCOUNT="$ACCOUNT" python3 -c '
import json, os
policy = {"Version": "2012-10-17", "Statement": [{
    "Sid": "WagerServiceOnly", "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::%s:user/wager-service" % os.environ["ACCOUNT"]},
    "Action": ["sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility",
               "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
    "Resource": os.environ["ARN"]}]}
print(json.dumps({"Policy": json.dumps(policy)}))')
  awslocal sqs set-queue-attributes --queue-url "$URL" --attributes "$ATTRS" >/dev/null
done

echo "queues provisioned:"
awslocal sqs list-queues
