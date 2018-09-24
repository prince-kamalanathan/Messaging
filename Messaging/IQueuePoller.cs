using System.Threading;
using System.Threading.Tasks;

namespace Messaging
{
    public interface IQueuePoller
    {
        Task PollAsync(CancellationToken cancellationToken);
    }
}