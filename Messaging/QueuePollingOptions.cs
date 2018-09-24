namespace Messaging
{
    public class QueuePollingOptions
    {
        public int MessageVisibilityTimeoutInSeconds { get; set; }

        public int PollingWaitTimeInSeconds { get; set; }

        public string QueueIdentifier { get; set; }
    }
}