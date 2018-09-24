using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace Messaging.Aws
{
    public class SqsQueuePoller : IQueuePoller
    {
        private readonly IAmazonSQS amazonSqs;
        private readonly ILogger<SqsQueuePoller> logger;
        private readonly IMessageDespatcher messageDespatcher;
        private readonly QueuePollingOptions queuePollingOptions;

        public SqsQueuePoller(
            IAmazonSQS amazonSqs,
            ILogger<SqsQueuePoller> logger,
            IMessageDespatcher messageDespatcher,
            QueuePollingOptions queuePollingOptions)
        {
            this.amazonSqs = amazonSqs;
            this.logger = logger;
            this.messageDespatcher = messageDespatcher;
            this.queuePollingOptions = queuePollingOptions;
        }

        public async Task PollAsync(CancellationToken cancellationToken)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                MaxNumberOfMessages = 1,
                MessageAttributeNames = { "All" },
                QueueUrl = queuePollingOptions.QueueIdentifier,
                VisibilityTimeout = queuePollingOptions.MessageVisibilityTimeoutInSeconds == 0
                    ? 43200
                    : queuePollingOptions.MessageVisibilityTimeoutInSeconds,
                WaitTimeSeconds = queuePollingOptions.PollingWaitTimeInSeconds
            };

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    logger.LogDebug($"Polling queue: {queuePollingOptions.QueueIdentifier}");
                    var receiveMessageResponse = await amazonSqs.ReceiveMessageAsync(receiveMessageRequest, cancellationToken);
                    foreach (var message in receiveMessageResponse.Messages)
                    {
                        var correlationId = GetCorrelationId(message);
                        using (logger.BeginScope(new Dictionary<string, object> { [MessageAttributes.CorrelationId] = correlationId }))
                        {
                            try
                            {
                                logger.LogDebug($"Despatching message: {message.MessageId}");
                                await messageDespatcher.DespatchAsync(correlationId, message);

                                logger.LogDebug($"Deleting message: {message.MessageId}");
                                await amazonSqs.DeleteMessageAsync(queuePollingOptions.QueueIdentifier, message.ReceiptHandle, cancellationToken);
                            }
                            catch (Exception exception)
                            {
                                await UnlockMessageAsync(message, cancellationToken);
                                logger.LogError(exception, $"Error while handling message: {message.MessageId}");
                            }
                        }
                    }
                }
                catch (Exception exception)
                {
                    logger.LogError(exception, $"Error while polling queue: {queuePollingOptions.QueueIdentifier}");
                }
            }
        }

        private static Guid GetCorrelationId(Message message)
        {
            if (message.MessageAttributes != null &&
                message.MessageAttributes.ContainsKey(MessageAttributes.CorrelationId) &&
                Guid.TryParse(message.MessageAttributes[MessageAttributes.CorrelationId].StringValue, out var correlationId))
            {
                return correlationId;
            }

            return Guid.NewGuid();
        }

        private async Task UnlockMessageAsync(Message message, CancellationToken cancellationToken)
        {
            await amazonSqs.ChangeMessageVisibilityAsync(
                new ChangeMessageVisibilityRequest
                {
                    ReceiptHandle = message.ReceiptHandle,
                    QueueUrl = queuePollingOptions.QueueIdentifier,
                    VisibilityTimeout = 0
                },
                cancellationToken);
        }
    }
}