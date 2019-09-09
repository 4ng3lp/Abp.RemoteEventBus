namespace Abp.RemoteEventBus.RabbitMQ
{
    public interface IRabbitMQSetting
    {
        bool AutomaticRecoveryEnabled { get; set; }
        string Url { get; set; }
        int InitialSize { get; set; }
        int MaxSize { get; set; }
        string HostName { get; set; }
        int Port { get; set; }
        string UserName { get; set; }
        string Password { get; set; }
    }
}