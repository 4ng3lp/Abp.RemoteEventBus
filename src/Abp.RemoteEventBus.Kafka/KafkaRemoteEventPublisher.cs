using System;
using Castle.Core.Logging;
using Confluent.Kafka;
using System.Text;
using System.Threading.Tasks;
using Abp.RemoteEventBus.Interface;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace Abp.RemoteEventBus.Kafka
{
    public class KafkaRemoteEventPublisher : IRemoteEventPublisher
    {
        public ILogger Logger { get; set; }

        private readonly IKafkaSetting _kafkaSetting;
        private readonly IRemoteEventSerializer _remoteEventSerializer;

        private readonly IProducer<string, string> _producer;

        private bool _disposed;

        public KafkaRemoteEventPublisher(
            [NotNull] IKafkaSetting kafkaSetting,
            [NotNull] IRemoteEventSerializer remoteEventSerializer)
        {
            if (kafkaSetting == null) throw new ArgumentNullException(nameof(kafkaSetting));
            if (remoteEventSerializer == null) throw new ArgumentNullException(nameof(remoteEventSerializer));
           
            Check.NotNullOrWhiteSpace(kafkaSetting.Properties["bootstrap.servers"] as string, "bootstrap.servers");

            _kafkaSetting = kafkaSetting;
            _remoteEventSerializer = remoteEventSerializer;

            Logger = NullLogger.Instance;

            var config = new ProducerConfig{ BootstrapServers = kafkaSetting.Properties["bootstrap.servers"] as string };

            _producer = new ProducerBuilder<string, string>(config)
                .Build();
        }

        public void Publish(string topic, IRemoteEventData remoteEventData)
        {
            PublishAsync(topic, remoteEventData);
            //_producer.Flush(TimeSpan.FromSeconds(10));
        }

        public Task PublishAsync(string topic, IRemoteEventData remoteEventData)
        {
            Logger.Debug($"{_producer.Name} producing on {topic}");
            
            var stringData = _remoteEventSerializer.Serialize(remoteEventData);

            var deliveryReport = _producer.ProduceAsync(topic,
                new Message<string, string>()
                {
                    Key = remoteEventData.Type,
                    Value = stringData
                });

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