using Momento.Sdk.Exceptions;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryDeleteResponse
{
    public class Success : CacheDictionaryDeleteResponse
    {
    }
    public class Error : CacheDictionaryDeleteResponse
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
