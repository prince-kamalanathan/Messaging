using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Newtonsoft.Json;
using Xunit;

namespace Messaging.Aws.Tests.Unit
{
    public class SqsMessageDespatcherTests
    {
        private readonly Mock<IServiceScopeFactory> serviceScopeFactoryMock;
        private readonly SqsMessageDespatcher sqsMessageDespatcher;

        public SqsMessageDespatcherTests()
        {
            serviceScopeFactoryMock = new Mock<IServiceScopeFactory>();
            sqsMessageDespatcher = new SqsMessageDespatcher(new List<Type> { typeof(TestMessage) }, serviceScopeFactoryMock.Object);
        }

        [Fact]
        public async Task Dispatch_Calls_Throws_Exception_When_Message_Type_Not_Found()
        {
            await Assert.ThrowsAsync<InvalidMessageContentsException>(
                async () => await sqsMessageDespatcher.DespatchAsync(Guid.Empty, new Message()));
        }

        [Fact]
        public async Task Dispatch_Calls_Throws_Exception_When_Handler_Not_Found()
        {
            var message = new Message
            {
                Body = JsonConvert.SerializeObject(new TestMessage()),
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {
                        MessageAttributes.AssemblyQualifiedName,
                        new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue = typeof(TestMessage).AssemblyQualifiedName
                        }
                    }
                }
            };

            var serviceProviderMock = new Mock<IServiceProvider>();

            var serviceScopeMock = new Mock<IServiceScope>();
            serviceScopeMock.Setup(p => p.ServiceProvider)
                .Returns(serviceProviderMock.Object);

            serviceScopeFactoryMock.Setup(p => p.CreateScope())
                .Returns(serviceScopeMock.Object);

            await Assert.ThrowsAsync<Exception>(
                async () => await sqsMessageDespatcher.DespatchAsync(Guid.Empty, message));
        }

        [Fact]
        public async Task Dispatch_Calls_Correct_Message_Handler_When_AssemblyQualifiedName_Is_Provided()
        {
            var message = new Message
            {
                Body = JsonConvert.SerializeObject(new TestMessage()),
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {
                        MessageAttributes.AssemblyQualifiedName,
                        new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue = typeof(TestMessage).AssemblyQualifiedName
                        }
                    }
                }
            };

            var mockMessageHandler = new Mock<IMessageHandler<TestMessage>>();

            var serviceProviderMock = new Mock<IServiceProvider>();
            serviceProviderMock.Setup(p => p.GetService(typeof(IMessageHandler<TestMessage>)))
                .Returns(mockMessageHandler.Object);

            var serviceScopeMock = new Mock<IServiceScope>();
            serviceScopeMock.Setup(p => p.ServiceProvider)
                .Returns(serviceProviderMock.Object);

            serviceScopeFactoryMock.Setup(p => p.CreateScope())
                .Returns(serviceScopeMock.Object);

            await sqsMessageDespatcher.DespatchAsync(Guid.Empty, message);

            mockMessageHandler.Verify(p => p.HandleAsync(Guid.Empty, It.IsAny<TestMessage>()), Times.Once);
        }

        [Fact]
        public async Task Dispatch_Calls_Correct_Message_Handler_When_Type_Is_Provided()
        {
            var message = new Message
            {
                Body = JsonConvert.SerializeObject(new TestMessage()),
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {
                        MessageAttributes.Type,
                        new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue = typeof(TestMessage).Name
                        }
                    }
                }
            };

            var mockMessageHandler = new Mock<IMessageHandler<TestMessage>>();

            var serviceProviderMock = new Mock<IServiceProvider>();
            serviceProviderMock.Setup(p => p.GetService(typeof(IMessageHandler<TestMessage>)))
                .Returns(mockMessageHandler.Object);

            var serviceScopeMock = new Mock<IServiceScope>();
            serviceScopeMock.Setup(p => p.ServiceProvider)
                .Returns(serviceProviderMock.Object);

            serviceScopeFactoryMock.Setup(p => p.CreateScope())
                .Returns(serviceScopeMock.Object);

            await sqsMessageDespatcher.DespatchAsync(Guid.Empty, message);

            mockMessageHandler.Verify(p => p.HandleAsync(Guid.Empty, It.IsAny<TestMessage>()), Times.Once);
        }
    }
}