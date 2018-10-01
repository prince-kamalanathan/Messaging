using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Moq;
using Newtonsoft.Json;
using Xunit;

namespace Messaging.Aws.Tests.Unit
{
    public class SqsMessageSenderTests
    {
        private readonly Mock<IAmazonSQS> amazonSqsMock;
        private readonly SqsMessageSender sqsMessageSender;

        public SqsMessageSenderTests()
        {
            amazonSqsMock = new Mock<IAmazonSQS>();
            sqsMessageSender = new SqsMessageSender(amazonSqsMock.Object);
        }

        [Fact]
        public async Task SendRequest_Should_Not_Have_Extra_Attributes()
        {
            var command = new TestMessage { Body = "body" };
            var assemblyQualifiedName = typeof(TestMessage).AssemblyQualifiedName;

            await sqsMessageSender.SendAsync("test_queue", command);

            amazonSqsMock.Verify(
                p => p.SendMessageAsync(It.Is<SendMessageRequest>(
                        q => q.MessageBody == JsonConvert.SerializeObject(command) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.AssemblyQualifiedName && r.Value.StringValue == assemblyQualifiedName) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.Type && r.Value.StringValue == typeof(TestMessage).Name) &&
                             q.QueueUrl == "test_queue"),
                    CancellationToken.None),
                Times.Once);
        }

        [Fact]
        public async Task SendRequest_Should_Have_Extra_Attributes()
        {
            var command = new TestMessage { Body = "body" };
            var assemblyQualifiedName = typeof(TestMessage).AssemblyQualifiedName;

            await sqsMessageSender.SendAsync("test_queue", command, new Dictionary<string, string> { { "A", "B" } });

            amazonSqsMock.Verify(
                p => p.SendMessageAsync(It.Is<SendMessageRequest>(
                        q => q.MessageBody == JsonConvert.SerializeObject(command) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.AssemblyQualifiedName && r.Value.StringValue == assemblyQualifiedName) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.Type && r.Value.StringValue == typeof(TestMessage).Name) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == "A" && r.Value.StringValue == "B") &&
                             q.QueueUrl == "test_queue"),
                    CancellationToken.None),
                Times.Once);
        }
    }
}