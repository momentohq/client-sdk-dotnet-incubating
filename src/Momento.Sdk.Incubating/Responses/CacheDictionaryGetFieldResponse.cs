using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryGetFieldResponse
{
    public class Hit : CacheDictionaryGetFieldResponse
    {
        protected readonly ByteString value;

        public Hit(_DictionaryGetResponse response)
        {
            this.value = response.Found.Items[0].CacheBody;
        }

        public Hit(ByteString cacheBody)
        {
            this.value = cacheBody;
        }

        public byte[] ValueByteArray
        {
            get => value.ToByteArray();
        }

        public string ValueString { get => value.ToStringUtf8(); }
    }

    public class Miss : CacheDictionaryGetFieldResponse
    {

    }

    public class Error : CacheDictionaryGetFieldResponse
    {
        private readonly SdkException _error;
        public Error(SdkException error)
        {
            _error = error;
        }

        public SdkException Exception
        {
            get => _error;
        }

        public MomentoErrorCode ErrorCode
        {
            get => _error.ErrorCode;
        }

        public string Message
        {
            get => $"{_error.MessageWrapper}: {_error.Message}";
        }

        public override string ToString()
        {
            return base.ToString() + ": " + Message;
        }
    }
}
