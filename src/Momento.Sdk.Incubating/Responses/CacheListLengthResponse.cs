using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheListLengthResponse
{
    public class Success : CacheListLengthResponse
    {
        public int ListLength { get; private set; }
        public Success(_ListLengthResponse response)
        {
            if (response.ListCase == _ListLengthResponse.ListOneofCase.Found)
            {
                Length = checked((int)response.Found.Length);
            }
        }
    }
    public class Error : CacheListLengthResponse
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
