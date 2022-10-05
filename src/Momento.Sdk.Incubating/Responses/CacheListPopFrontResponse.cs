using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheListPopFrontResponse
{
    public class Hit : CacheListPopFrontResponse
    {
        protected readonly ByteString value;

        public Hit(_ListPopFrontResponse response)
        {
            this.value = response.Found.Front;
        }

        public byte[] ByteArray
        {
            get => value.ToByteArray();
        }

        public string String() => value.ToStringUtf8();
    }

    public class Miss : CacheListPopFrontResponse
    {
        public Miss() { }
        public byte[]? ByteArray
        {
            get
            {
                return null;
            }
        }

        public string? String() => null;
    }

    public class Error : CacheListPopFrontResponse
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
