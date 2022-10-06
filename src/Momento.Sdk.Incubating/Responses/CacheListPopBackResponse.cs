using Google.Protobuf;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheListPopBackResponse
{
    public class Hit : CacheListPopBackResponse
    {
        protected readonly ByteString value;

        public Hit(_ListPopBackResponse response)
        {
            this.value = response.Found.Back;
        }

        public byte[] ByteArray
        {
            get => value.ToByteArray();
        }

        public string String() => value.ToStringUtf8();
    }

    public class Miss : CacheListPopBackResponse
    {

    }

    public class Error : CacheListPopBackResponse
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

    }
}
