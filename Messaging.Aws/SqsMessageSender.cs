using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;

namespace Messaging.Aws
{
    public class SqsMessageSender : IMessageSender
    {
        private readonly IAmazonSQS amazonSqs;

        public SqsMessageSender(IAmazonSQS amazonSqs)
        {
            this.amazonSqs = amazonSqs;
        }

        public async Task SendAsync(string queueIdentifier, object message)
        {
            await amazonSqs.SendMessageAsync(GetSendMessageRequest(queueIdentifier, message));
        }

        public async Task SendAsync(string queueIdentifier, string messageId, object message)
        {
            await amazonSqs.SendMessageAsync(GetSendMessageRequest(queueIdentifier, messageId, message));
        }

        public async Task SendAsync(string queueIdentifier, object message, Dictionary<string, string> messageAttributes)
        {
            var sendMessageRequest = GetSendMessageRequest(queueIdentifier, message);
            foreach (var attribute in messageAttributes)
            {
                sendMessageRequest.MessageAttributes.Add(
                    attribute.Key, new MessageAttributeValue { DataType = "String", StringValue = attribute.Value });
            }

            await amazonSqs.SendMessageAsync(sendMessageRequest);
        }

        public async Task SendAsync(string queueIdentifier, string messageId, object message, Dictionary<string, string> messageAttributes)
        {
            var sendMessageRequest = GetSendMessageRequest(queueIdentifier, messageId, message);
            foreach (var attribute in messageAttributes)
            {
                sendMessageRequest.MessageAttributes.Add(
                    attribute.Key, new MessageAttributeValue { DataType = "String", StringValue = attribute.Value });
            }

            await amazonSqs.SendMessageAsync(sendMessageRequest);
        }

        private static SendMessageRequest GetSendMessageRequest(string queueIdentifier, object message)
        {
            return new SendMessageRequest
            {
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
                MessageBody = JsonConvert.SerializeObject(message),
                QueueUrl = queueIdentifier
            };
        }

        private static SendMessageRequest GetSendMessageRequest(string queueIdentifier, string messageId, object message)
        {
            var sendMessageRequest = GetSendMessageRequest(queueIdentifier, message);
            sendMessageRequest.MessageDeduplicationId = messageId;

            return sendMessageRequest;
        }
    }
}