using Castle.Core.Logging;
using Confluent.Kafka;
using System.Text;
using System.Threading.Tasks;
using Abp.RemoteEventBus.Interface;
using System.Collections.Generic;

namespace Abp.RemoteEventBus.Kafka
{
    public class KafkaRemoteEventPublisher : IRemoteEventPublisher
    {
        public ILogger Logger { get; set; }

        private readonly IRemoteEventSerializer _remoteEventSerializer;

        private readonly IProducer<string, IDictionary<string, object>> _producer;

        private bool _disposed;

        public KafkaRemoteEventPublisher(IKafkaSetting kafkaSetting, IRemoteEventSerializer remoteEventSerializer)
        {
            Check.NotNullOrWhiteSpace(kafkaSetting.Properties["bootstrap.servers"] as string, "bootstrap.servers");

            _remoteEventSerializer = remoteEventSerializer;

            Logger = NullLogger.Instance;

            var config = new ProducerConfig { BootstrapServers = kafkaSetting.Properties["bootstrap.servers"] as string };

            _producer = new ProducerBuilder<string, IDictionary<string, object>>(config).Build();

        }

        public void Publish(string topic, IRemoteEventData remoteEventData)
        {
            PublishAsync(topic, remoteEventData);
            //_producer.Flush(TimeSpan.FromSeconds(10));
        }

        public Task PublishAsync(string topic, IRemoteEventData remoteEventData)
        {
            Logger.Debug($"{_producer.Name} producing on {topic}");
            var foo = _remoteEventSerializer.Serialize(remoteEventData);

            var deliveryReport = _producer.ProduceAsync(topic, new Message<string, IDictionary<string, object>>(){}  );

            return deliveryReport.ContinueWith(task =>
            {
                Logger.Debug($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
            });
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _producer?.Dispose();
                _disposed = true;
            }
        }
    }
}