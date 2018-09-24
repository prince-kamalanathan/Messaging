using System.Collections.Generic;
using System.Threading.Tasks;

namespace Messaging
{
    public interface IMessagePublisher
    {
        Task PublishAsync(string topicIdentifier, object message);

        Task PublishAsync(string topicIdentifier, object message, Dictionary<string, string> messageAttributes);
    }
}