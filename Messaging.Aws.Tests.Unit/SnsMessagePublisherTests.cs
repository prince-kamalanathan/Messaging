using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Moq;
using Newtonsoft.Json;
using Xunit;

namespace Messaging.Aws.Tests.Unit
{
    public class SnsMessagePublisherTests
    {
        private readonly Mock<IAmazonSimpleNotificationService> amazonSimpleNotificationServiceMock;
        private readonly SnsMessagePublisher smsMessagePublisher;

        public SnsMessagePublisherTests()
        {
            amazonSimpleNotificationServiceMock = new Mock<IAmazonSimpleNotificationService>();
            smsMessagePublisher = new SnsMessagePublisher(amazonSimpleNotificationServiceMock.Object);
        }

        [Fact]
        public async Task PublishRequest_Should_Not_Have_Extra_Attributes()
        {
            var message = new TestMessage { Body = "body" };
            var assemblyQualifiedName = typeof(TestMessage).AssemblyQualifiedName;

            await smsMessagePublisher.PublishAsync("test_topic", message);

            amazonSimpleNotificationServiceMock.Verify(
                p => p.PublishAsync(It.Is<PublishRequest>(
                        q => q.Message == JsonConvert.SerializeObject(message) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.AssemblyQualifiedName && r.Value.StringValue == assemblyQualifiedName) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.Type && r.Value.StringValue == typeof(TestMessage).Name) &&
                             q.TopicArn == "test_topic"),
                    CancellationToken.None),
                Times.Once);
        }

        [Fact]
        public async Task PublishRequest_Should_Have_Extra_Attributes()
        {
            var message = new TestMessage { Body = "body" };
            var assemblyQualifiedName = typeof(TestMessage).AssemblyQualifiedName;

            await smsMessagePublisher.PublishAsync("test_topic", message, new Dictionary<string, string> { { "A", "B" } });

            amazonSimpleNotificationServiceMock.Verify(
                p => p.PublishAsync(It.Is<PublishRequest>(
                        q => q.Message == JsonConvert.SerializeObject(message) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.AssemblyQualifiedName && r.Value.StringValue == assemblyQualifiedName) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == MessageAttributes.Type && r.Value.StringValue == typeof(TestMessage).Name) &&
                             q.MessageAttributes.Any(r =>
                                 r.Key == "A" && r.Value.StringValue == "B") &&
                             q.TopicArn == "test_topic"),
                    CancellationToken.None),
                Times.Once);
        }
    }
}