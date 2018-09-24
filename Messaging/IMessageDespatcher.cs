using System;
using System.Threading.Tasks;

namespace Messaging
{
    public interface IMessageDespatcher
    {
        Task DespatchAsync(Guid correlationId, object message);
    }
}