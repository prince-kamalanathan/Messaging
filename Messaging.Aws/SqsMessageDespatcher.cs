using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;

namespace Messaging.Aws
{
    public class SqsMessageDespatcher : IMessageDespatcher
    {
        private readonly IEnumerable<Type> types;
        private readonly IServiceScopeFactory serviceScopeFactory;

        public SqsMessageDespatcher(IEnumerable<Type> types, IServiceScopeFactory serviceScopeFactory)
        {
            this.types = types;
            this.serviceScopeFactory = serviceScopeFactory;
        }

        public async Task DespatchAsync(Guid correlationId, object message)
        {
            using (var serviceScope = serviceScopeFactory.CreateScope())
            {
                var messageToHandle = GetMessageToHandle(message);
                var messageHandlerType = typeof(IMessageHandler<>).MakeGenericType(messageToHandle.GetType());
                var messageHandler = serviceScope.ServiceProvider.GetService(messageHandlerType);
                if (messageHandler != null)
                {
                    var handleMethod = messageHandlerType.GetMethod("HandleAsync", BindingFlags.Public | BindingFlags.Instance);
                    if (handleMethod != null)
                    {
                        var task = (Task)handleMethod.Invoke(messageHandler, new[] { correlationId, messageToHandle });
                        await task;
                    }
                }
                else
                {
                    throw new Exception($"Unable to find IMessageHandler for {messageToHandle.GetType()}");
                }
            }
        }

        private object GetMessageToHandle(object message)
        {
            var sqsMessage = (Message)message;
            if ((!sqsMessage.MessageAttributes.ContainsKey(MessageAttributes.AssemblyQualifiedName) ||
                 string.IsNullOrWhiteSpace(sqsMessage.MessageAttributes[MessageAttributes.AssemblyQualifiedName].StringValue)) &&
                (!sqsMessage.MessageAttributes.ContainsKey(MessageAttributes.Type) ||
                 string.IsNullOrWhiteSpace(sqsMessage.MessageAttributes[MessageAttributes.Type].StringValue)))
            {
                throw new InvalidMessageContentsException($"Unable to despatch message: {sqsMessage.MessageId}");
            }

            Type messageType = null;
            string messageTypeAttribute;
            if (sqsMessage.MessageAttributes.ContainsKey(MessageAttributes.AssemblyQualifiedName))
            {
                messageTypeAttribute = sqsMessage.MessageAttributes[MessageAttributes.AssemblyQualifiedName].StringValue;
                messageType = Type.GetType(messageTypeAttribute);
            }

            if (messageType != null)
            {
                return JsonConvert.DeserializeObject(sqsMessage.Body, messageType);
            }

            messageTypeAttribute = sqsMessage.MessageAttributes[MessageAttributes.Type].StringValue;
            messageType = types.First(p => p.Name == messageTypeAttribute);

            return JsonConvert.DeserializeObject(sqsMessage.Body, messageType);
        }
    }
}