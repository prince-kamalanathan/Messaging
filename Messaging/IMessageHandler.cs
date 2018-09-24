using System;
using System.Threading.Tasks;

namespace Messaging
{
    public interface IMessageHandler<in T>
    {
        Task HandleAsync(Guid correlationId, T message);
    }
}