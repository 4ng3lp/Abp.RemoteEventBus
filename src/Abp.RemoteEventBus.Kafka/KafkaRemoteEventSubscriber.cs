using Castle.Core.Logging;
using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Abp.RemoteEventBus.Kafka
{
    public class KafkaRemoteEventSubscriber : IRemoteEventSubscriber
    {
        public ILogger Logger { get; set; }

        private readonly ConcurrentDictionary<string, IConsumer<Ignore, string>> _dictionary;

        private readonly IKafkaSetting _kafkaSetting;

        private bool _cancelled;

        private bool _disposed;

        public KafkaRemoteEventSubscriber(IKafkaSetting kafkaSetting)
        {
            Check.NotNullOrWhiteSpace(kafkaSetting.Properties["bootstrap.servers"] as string, "bootstrap.servers");

            _kafkaSetting = kafkaSetting;

            _dictionary = new ConcurrentDictionary<string, IConsumer<Ignore, string>>();

            Logger = NullLogger.Instance;
        }

        public void Subscribe(IEnumerable<string> topics, Action<string, string> handler)
        {
            var existsTopics = topics.ToList().Where(p => _dictionary.ContainsKey(p));
 
            if (existsTopics.Any())
            {
                return;
                //TODO разобраться, почему пробрасывается  Exception
                //throw new AbpException(string.Format("Topics {0} have subscribed already", string.Join(",", existsTopics)));
            }

            topics.ToList().ForEach(topic =>
            {
                var conf = new ConsumerConfig
                {
                    GroupId = "test-consumer-group",
                    BootstrapServers = (string)_kafkaSetting.Properties["bootstrap.servers"],
                    // Note: The AutoOffsetReset property determines the start offset in the event
                    // there are not yet any committed offsets for the consumer group for the
                    // topic/partitions of interest. By default, offsets are committed
                    // automatically, so in this example, consumption will only start from the
                    // earliest message in the topic 'my-topic' the first time you run the program.
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };
                var consumer = new ConsumerBuilder<Ignore, string>(conf)
                    .SetErrorHandler((_, e) => Logger.Error($"Error: {e.Reason}"))
                    .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                        // possibly manually specify start offsets or override the partition assignment provided by
                        // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                        // 
                        // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
                    })
                    .Build();

                _dictionary[topic] = consumer;

                consumer.Subscribe(topics);

                try
                {
                    while (!_cancelled)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume();

                            if (consumeResult.IsPartitionEOF)
                            {
                                Debug.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }
                            try
                            {
                                handler(consumeResult.Topic, consumeResult.Message.Value);
                            }
                            catch (Exception ex)
                            {
                                Logger.Error($"Handler error", ex);
                            }

                            Debug.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Value}");

                            if (consumeResult.Offset % Convert.ToInt32(_kafkaSetting.Properties["commit.period"]) == 0)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of failure.
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    Logger.Error($"Commit error", e);
                                    Debug.WriteLine($"Commit error: {e.Error.Reason}");
                                }
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Logger.Error($"Consume error: {e.Error.Reason}");
                            Debug.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Debug.WriteLine("Closing consumer.");
                    consumer.Close();
                }

            });
        }

        public Task SubscribeAsync(IEnumerable<string> topics, Action<string, string> handler)
        {
            return Task.Factory.StartNew(() =>
             {
                 Subscribe(topics, handler);
             });
        }

        public void Unsubscribe(IEnumerable<string> topics)
        {
            _dictionary.Where(p => topics.Contains(p.Key)).Select(p => p.Value).ToList().ForEach(p => p.Unsubscribe());
        }

        public Task UnsubscribeAsync(IEnumerable<string> topics)
        {
            return Task.Factory.StartNew(() => Unsubscribe(topics));
        }

        public void UnsubscribeAll()
        {
            _dictionary.Select(p => p.Value).ToList().ForEach(p => p.Unsubscribe());
        }

        public Task UnsubscribeAllAsync()
        {
            return Task.Factory.StartNew(() => UnsubscribeAll());
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _cancelled = true;
                UnsubscribeAll();
                _dictionary.Select(p => p.Value).ToList().ForEach(consumer => consumer?.Dispose());

                _disposed = true;
            }
        }
    }
}
