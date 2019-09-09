using Commons.Pool;
using RabbitMQ.Client;
using System;

namespace Abp.RemoteEventBus.RabbitMQ
{
    public class PooledObjectFactory : IPooledObjectFactory<IConnection>
    {
        private readonly ConnectionFactory _connectionFactory;

        public PooledObjectFactory(IRabbitMQSetting rabbitMQSetting)
        {
            _connectionFactory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true
            };
            if (!string.IsNullOrWhiteSpace(rabbitMQSetting.Url))
            {
                var uri = new Uri(rabbitMQSetting.Url);
                _connectionFactory.Uri = uri;
            }
            else
            {
                _connectionFactory.HostName = rabbitMQSetting.HostName;
                _connectionFactory.Port = rabbitMQSetting.Port; //端口号
                _connectionFactory.UserName = rabbitMQSetting.UserName; //用户账号
                _connectionFactory.Password = rabbitMQSetting.Password; //用户密码
            }
            _connectionFactory.Protocol = Protocols.DefaultProtocol;
            _connectionFactory.AutomaticRecoveryEnabled = true; //自动重连
            _connectionFactory.RequestedFrameMax = UInt32.MaxValue;
            _connectionFactory.RequestedHeartbeat = UInt16.MaxValue; //心跳超时时间
        }

        public IConnection Create()
        {
            return _connectionFactory.CreateConnection();
        }

        public void Destroy(IConnection obj)
        {
            obj.Dispose();
        }
    }
}