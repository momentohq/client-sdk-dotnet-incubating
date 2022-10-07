using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryIncrementResponse
{
    public class Success : CacheDictionaryIncrementResponse
    {
        public long Value { get; private set; }
        public Success(_DictionaryIncrementResponse response)
        {
            Value = response.Value;
        }
    }

    public class Error : CacheDictionaryIncrementResponse
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
