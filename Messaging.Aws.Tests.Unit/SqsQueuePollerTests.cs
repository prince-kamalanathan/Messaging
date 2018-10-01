using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Messaging.Aws.Tests.Unit
{
    public class SqsQueuePollerTests
    {
        private readonly Mock<IAmazonSQS> amazonSqsMock;
        private readonly Mock<IMessageDespatcher> messageDespatcherMock;
        private readonly SqsQueuePoller sqsQueuePoller;

        public SqsQueuePollerTests()
        {
            amazonSqsMock = new Mock<IAmazonSQS>();
            messageDespatcherMock = new Mock<IMessageDespatcher>();
            sqsQueuePoller = new SqsQueuePoller(
                amazonSqsMock.Object,
                new Mock<ILogger<SqsQueuePoller>>().Object,
                messageDespatcherMock.Object,
                new QueuePollingOptions
                {
                    MessageVisibilityTimeoutInSeconds = 30,
                    PollingWaitTimeInSeconds = 20,
                    QueueIdentifier = "test_queue"
                });
        }

        [Fact]
        public async Task PollAsync()
        {
            amazonSqsMock.SetupSequence(p => p.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()))
                .Throws<Exception>()
                .Returns(
                    Task.FromResult(
                        new ReceiveMessageResponse
                        {
                            Messages = new List<Message>
                            {
                                new Message
                                {
                                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                                    {
                                        {
                                            MessageAttributes.CorrelationId,
                                            new MessageAttributeValue
                                            {
                                                StringValue = "ce4699f6-6725-4707-b635-0964daad63f1"
                                            }
                                        }
                                    }
                                }
                            }
                        }))
                .Returns(
                    Task.FromResult(
                        new ReceiveMessageResponse
                        {
                            Messages = new List<Message>
                            {
                                new Message
                                {
                                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                                    {
                                        {
                                            MessageAttributes.CorrelationId,
                                            new MessageAttributeValue
                                            {
                                                StringValue = "ce4699f6-6725-4707-b635-0964daad63f2"
                                            }
                                        }
                                    }
                                }
                            }
                        }));

            messageDespatcherMock.SetupSequence(p => p.DespatchAsync(Guid.Empty, It.IsAny<object>()))
                .Throws(new Exception("test"))
                .Returns(Task.CompletedTask);

            var cancellationTokenSource = new CancellationTokenSource();

            Task.Run(async () => await sqsQueuePoller.PollAsync(cancellationTokenSource.Token));

            await Task.Delay(1000, cancellationTokenSource.Token);
            cancellationTokenSource.Cancel();

            amazonSqsMock.Verify(
                p => p.ReceiveMessageAsync(It.IsAny<ReceiveMessageRequest>(), It.IsAny<CancellationToken>()), Times.AtLeast(3));
            messageDespatcherMock.Verify(
                p => p.DespatchAsync(Guid.Parse("ce4699f6-6725-4707-b635-0964daad63f1"), It.IsAny<object>()), Times.Once);
            messageDespatcherMock.Verify(
                p => p.DespatchAsync(Guid.Parse("ce4699f6-6725-4707-b635-0964daad63f2"), It.IsAny<object>()), Times.Once);
        }
    }
}