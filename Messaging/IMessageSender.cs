using System.Collections.Generic;
using System.Threading.Tasks;

namespace Messaging
{
    public interface IMessageSender
    {
        Task SendAsync(string queueIdentifier, object message);

        Task SendAsync(string queueIdentifier, string messageId, object message);

        Task SendAsync(string queueIdentifier, object message, Dictionary<string, string> messageAttributes);

        Task SendAsync(string queueIdentifier, string messageId, object message, Dictionary<string, string> messageAttributes);
    }
}