interface IHandler
{
    TimeSpan Timeout { get; }

    Task PerformOperation(CancellationToken cancellationToken);
}

class Handler : IHandler
{
    private readonly IConsumer _consumer;
    private readonly IPublisher _publisher;
    //private readonly ILogger<Handler> _logger;

    public TimeSpan Timeout { get; }

    public Handler(
      TimeSpan timeout,
      IConsumer consumer,
      IPublisher publisher
        //,      Ilogger<Handler> logger
        )
    {
        Timeout = timeout;

        _consumer = consumer;
        _publisher = publisher;
        //_logger = logger;
    }

    public Task PerformOperation(CancellationToken cancellationToken)
    {
        //TODO: place code here
        //while (true)
        {
            var t = _consumer.ReadData();
            t.Wait(cancellationToken);
            var event1 = t.Result;
            foreach (var address in event1.Recipients)
            {
                var task = Task.Run(() =>
                {
                    while (true)
                    {
                        var sendTask = _publisher.SendData(address, event1.Payload);
                        sendTask.Wait();
                        if (sendTask.Result == SendResult.Accepted) break;
                        Thread.Sleep((int)this.Timeout.TotalMilliseconds);
                    }
                });
                task.Start();
            }
        }
        return Task.CompletedTask;
    }
}

record Payload(string Origin, byte[] Data);
record Address(string DataCenter, string NodeId);
record Event(IReadOnlyCollection<Address> Recipients, Payload Payload);

enum SendResult
{
    Accepted,
    Rejected
}

interface IConsumer
{
    Task<Event> ReadData();
}

interface IPublisher
{
    Task<SendResult> SendData(Address address, Payload payload);
}