namespace Abp.RemoteEventBus.Interface
{
    public interface IRemoteEventSerializer
    {
        T Deserialize<T>(string value);

        string Serialize(object value);
    }
}