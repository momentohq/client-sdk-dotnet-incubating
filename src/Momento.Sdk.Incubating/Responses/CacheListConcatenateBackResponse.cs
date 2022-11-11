using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;

namespace Momento.Sdk.Incubating.Responses;

/// <summary>
/// The result of a <c>ListConcatenateBack</c> command
/// </summary>
///
public abstract class CacheListConcatenateBackResponse
{
    public class Success : CacheListConcatenateBackResponse
    {
        public int ListLength { get; private set; }
        public Success(_ListConcatenateBackResponse response)
        {
            ListLength = checked((int)response.ListLength);
        }
    }
    public class Error : CacheListConcatenateBackResponse
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
