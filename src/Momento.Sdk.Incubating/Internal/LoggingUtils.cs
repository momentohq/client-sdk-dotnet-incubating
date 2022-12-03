using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Momento.Protos.CacheClient;
using Momento.Sdk.Incubating.Requests;

namespace Momento.Sdk.Incubating.Internal
{
    // TODO: these should be merged into the base LoggingUtils class in the main SDK when data structures go 1.0.


    /// <summary>
    /// Utils for logging debug information about Momento requests and responses
    /// </summary>
    public static class LoggingUtils
    {
        /// <summary>
        /// Logs a message at TRACE level that indicates that a request is about to be executed
        /// </summary>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        public static void LogTraceExecutingCollectionRequest(this ILogger _logger, string requestType, string cacheName, string collectionName)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Executing '{}' request: cacheName: {}; collectionName: {}", requestType, cacheName, collectionName);
            }
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request is about to be executed
        /// </summary>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="field"></param>
        /// <param name="ttl"></param>
        public static void LogTraceExecutingCollectionRequest(this ILogger _logger, string requestType, string cacheName, string collectionName, string field, CollectionTtl? ttl)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Executing '{}' request: cacheName: {}; collectionName: {}; field: {}; ttl: {}", requestType, cacheName, collectionName, field, ttl);
            }
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request is about to be executed
        /// </summary>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="field"></param>
        /// <param name="ttl"></param>
        public static void LogTraceExecutingCollectionRequest(this ILogger _logger, string requestType, string cacheName, string collectionName, ByteString field, CollectionTtl? ttl)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableField = ReadableByteString(field);
                _logger.LogTrace("Executing '{}' request: cacheName: {}; collectionName: {}; fields: {}; ttl: {}", requestType, cacheName, collectionName, readableField, ttl);
            }
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request is about to be executed
        /// </summary>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="fields"></param>
        /// <param name="ttl"></param>
        public static void LogTraceExecutingCollectionRequest(this ILogger _logger, string requestType, string cacheName, string collectionName, IEnumerable<ByteString> fields, CollectionTtl? ttl)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableFields = String.Join(", ", fields.Select(k => ReadableByteString(k)));
                _logger.LogTrace("Executing '{}' request: cacheName: {}; collectionName: {}; fields: {}; ttl: {}", requestType, cacheName, collectionName, readableFields, ttl);
            }
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request is about to be executed
        /// </summary>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="items"></param>
        /// <param name="ttl"></param>
        public static void LogTraceExecutingCollectionRequest(this ILogger _logger, string requestType, string cacheName, string collectionName, IEnumerable<_DictionaryFieldValuePair> items, CollectionTtl? ttl)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableItems = String.Join(", ", items.Select(i => "(" + ReadableByteString(i.Field) + " -> " + ReadableByteString(i.Value) + ")"));
                _logger.LogTrace("Executing '{}' request: cacheName: {}; collectionName: {}; items: {}; ttl: {}", requestType, cacheName, collectionName, readableItems, ttl);
            }
        }


        /// <summary>
        /// Logs a message at TRACE level that indicates that a collection request resulted in an error.
        /// </summary>
        /// <typeparam name="TError"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static TError LogTraceCollectionRequestError<TError>(this ILogger _logger, string requestType, string cacheName, string collectionName, TError error)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("An error occurred while executing a '{}' request: cacheName: {}; collectionName: {}; error: {}", requestType, cacheName, collectionName, error);
            }
            return error;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a collection request resulted in an error.
        /// </summary>
        /// <typeparam name="TError"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="field"></param>
        /// <param name="ttl"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static TError LogTraceCollectionRequestError<TError>(this ILogger _logger, string requestType, string cacheName, string collectionName, string field, CollectionTtl ttl, TError error)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("An error occurred while executing a '{}' request: cacheName: {}; collectionName: {}; field: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, field, ttl, error);
            }
            return error;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a collection request resulted in an error.
        /// </summary>
        /// <typeparam name="TError"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="field"></param>
        /// <param name="ttl"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static TError LogTraceCollectionRequestError<TError>(this ILogger _logger, string requestType, string cacheName, string collectionName, ByteString field, CollectionTtl? ttl, TError error)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableField = ReadableByteString(field);
                _logger.LogTrace("An error occurred while executing a '{}' request: cacheName: {}; collectionName: {}; field: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, readableField, ttl, error);
            }
            return error;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a collection request resulted in an error.
        /// </summary>
        /// <typeparam name="TError"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="fields"></param>
        /// <param name="ttl"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static TError LogTraceCollectionRequestError<TError>(this ILogger _logger, string requestType, string cacheName, string collectionName, IEnumerable<ByteString> fields, CollectionTtl? ttl, TError error)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableFields = String.Join(", ", fields.Select(k => ReadableByteString(k)));
                _logger.LogTrace("An error occurred while executing a '{}' request: cacheName: {}; collectionName: {}; fields: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, readableFields, ttl, error);
            }
            return error;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a collection request resulted in an error.
        /// </summary>
        /// <typeparam name="TError"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="items"></param>
        /// <param name="ttl"></param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static TError LogTraceCollectionRequestError<TError>(this ILogger _logger, string requestType, string cacheName, string collectionName, IEnumerable<_DictionaryFieldValuePair> items, CollectionTtl ttl, TError error)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableItems = String.Join(", ", items.Select(i => "(" + ReadableByteString(i.Field) + " -> " + ReadableByteString(i.Value) + ")"));
                _logger.LogTrace("An error occurred while executing a '{}' request: cacheName: {}; collectionName: {}; fields: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, readableItems, ttl, error);
            }
            return error;
        }



        /// <summary>
        /// Logs a message at TRACE level that indicates that a request was successfully executed.
        /// </summary>
        /// <typeparam name="TSuccess"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="success"></param>
        /// <returns></returns>
        public static TSuccess LogTraceCollectionRequestSuccess<TSuccess>(this ILogger _logger, string requestType, string cacheName, string collectionName, TSuccess success)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Successfully executed '{}' request: cacheName: {}; collectionName: {}; error: {}", requestType, cacheName, collectionName, success);
            }
            return success;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request was successfully executed.
        /// </summary>
        /// <typeparam name="TSuccess"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="field"></param>
        /// <param name="ttl"></param>
        /// <param name="success"></param>
        /// <returns></returns>
        public static TSuccess LogTraceCollectionRequestSuccess<TSuccess>(this ILogger _logger, string requestType, string cacheName, string collectionName, string field, CollectionTtl ttl, TSuccess success)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Successfully executed '{}' request: cacheName: {}; collectionName: {}; field: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, field, ttl, success);
            }
            return success;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request was successfully executed.
        /// </summary>
        /// <typeparam name="TSuccess"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="field"></param>
        /// <param name="ttl"></param>
        /// <param name="success"></param>
        /// <returns></returns>
        public static TSuccess LogTraceCollectionRequestSuccess<TSuccess>(this ILogger _logger, string requestType, string cacheName, string collectionName, ByteString field, CollectionTtl? ttl, TSuccess success)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableField = ReadableByteString(field);
                _logger.LogTrace("Successfully executed '{}' request: cacheName: {}; collectionName: {}; field: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, readableField, ttl, success);
            }
            return success;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request was successfully executed.
        /// </summary>
        /// <typeparam name="TSuccess"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="fields"></param>
        /// <param name="ttl"></param>
        /// <param name="success"></param>
        /// <returns></returns>
        public static TSuccess LogTraceCollectionRequestSuccess<TSuccess>(this ILogger _logger, string requestType, string cacheName, string collectionName, IEnumerable<ByteString> fields, CollectionTtl? ttl, TSuccess success)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableFields = String.Join(", ", fields.Select(k => ReadableByteString(k)));
                _logger.LogTrace("Successfully executed '{}' request: cacheName: {}; collectionName: {}; fields: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, readableFields, ttl, success);
            }
            return success;
        }

        /// <summary>
        /// Logs a message at TRACE level that indicates that a request was successfully executed.
        /// </summary>
        /// <typeparam name="TSuccess"></typeparam>
        /// <param name="_logger"></param>
        /// <param name="requestType"></param>
        /// <param name="cacheName"></param>
        /// <param name="collectionName"></param>
        /// <param name="items"></param>
        /// <param name="ttl"></param>
        /// <param name="success"></param>
        /// <returns></returns>
        public static TSuccess LogTraceCollectionRequestSuccess<TSuccess>(this ILogger _logger, string requestType, string cacheName, string collectionName, IEnumerable<_DictionaryFieldValuePair> items, CollectionTtl ttl, TSuccess success)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                var readableItems = String.Join(", ", items.Select(i => "(" + ReadableByteString(i.Field) + " -> " + ReadableByteString(i.Value) + ")"));
                _logger.LogTrace("Successfully executed '{}' request: cacheName: {}; collectionName: {}; fields: {}; ttl: {}; error: {}", requestType, cacheName, collectionName, readableItems, ttl, success);
            }
            return success;
        }

        private static string ReadableByteString(ByteString? input)
        {
            if (input == null)
            {
                return "(null)";
            }
            var readable = input.ToStringUtf8();
            if (readable.Length <= 20)
            {
                return readable;
            }
            return readable.Substring(0, 20) + "...";
        }
    }
}

