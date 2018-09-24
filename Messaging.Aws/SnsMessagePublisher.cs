using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Newtonsoft.Json;

namespace Messaging.Aws
{
    public class SnsMessagePublisher : IMessagePublisher
    {
        private readonly IAmazonSimpleNotificationService amazonSimpleNotificationService;

        public SnsMessagePublisher(IAmazonSimpleNotificationService amazonSimpleNotificationService)
        {
            this.amazonSimpleNotificationService = amazonSimpleNotificationService;
        }

        public async Task PublishAsync(string topicIdentifier, object message)
        {
            await amazonSimpleNotificationService.PublishAsync(GetPublishRequest(topicIdentifier, message));
        }

        public async Task PublishAsync(string topicIdentifier, object message, Dictionary<string, string> messageAttributes)
        {
            var publishRequest = GetPublishRequest(topicIdentifier, message);
            foreach (var attribute in messageAttributes)
            {
                publishRequest.MessageAttributes.Add(
                    attribute.Key, new MessageAttributeValue { DataType = "String", StringValue = attribute.Value });
            }

            await amazonSimpleNotificationService.PublishAsync(publishRequest);
        }

        private static PublishRequest GetPublishRequest(string topicIdentifier, object message)
        {
            return new PublishRequest
            {
                Message = JsonConvert.SerializeObject(message),
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {
                        MessageAttributes.AssemblyQualifiedName,
                        new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue = message.GetType().AssemblyQualifiedName
                        }
                    },
                    {
                        MessageAttributes.Type,
                        new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue = message.GetType().Name
                        }
                    }
                },
                TopicArn = topicIdentifier
            };
        }
    }
}