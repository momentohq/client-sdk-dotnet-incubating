using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Momento.Sdk.Auth;
using Momento.Sdk.Config;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Incubating.Internal;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Responses;
using Utils = Momento.Sdk.Internal.Utils;

namespace Momento.Sdk.Incubating;

/// <summary>
/// Incubating cache client.
///
/// This enables preview features not ready for general release.
/// </summary>
public class SimpleCacheClient : ISimpleCacheClient
{
    private readonly ISimpleCacheClient simpleCacheClient;
    private readonly ScsDataClient dataClient;
    protected readonly IConfiguration config;
    protected readonly ILogger _logger;

    /// <summary>
    /// Client to perform operations against the Simple Cache Service.
    /// 
    /// Enables preview features.
    /// </summary>
    /// <param name="simpleCacheClient">Instance of release cache client to delegate operations to.</param>
    /// <param name="config">Configuration to use for the transport, retries, middlewares. See <see href="https://github.com/momentohq/client-sdk-dotnet/blob/main/src/Momento.Sdk/Config/Configurations.cs"/> for out-of-the-box configuration choices, eg <see href="https://github.com/momentohq/client-sdk-dotnet/blob/main/src/Momento.Sdk/Config/Configurations.cs#L22"/></param>
    /// <param name="authProvider">Momento JWT.</param>
    /// <param name="defaultTtl">Default time to live for the item in cache.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="defaultTtl"/> is zero or negative.</exception>
    public SimpleCacheClient(ISimpleCacheClient simpleCacheClient, IConfiguration config, ICredentialProvider authProvider, TimeSpan defaultTtl)
    {
        this.simpleCacheClient = simpleCacheClient;
        this.config = config;
        this._logger = config.LoggerFactory.CreateLogger<SimpleCacheClient>();
        Utils.ArgumentStrictlyPositive(defaultTtl, "defaultTtl");
        this.dataClient = new(config, authProvider.AuthToken, authProvider.CacheEndpoint, defaultTtl);
    }

    /// <inheritdoc />
    public async Task<CreateCacheResponse> CreateCacheAsync(string cacheName)
    {
        return await this.simpleCacheClient.CreateCacheAsync(cacheName);
    }

    /// <inheritdoc />
    public async Task<DeleteCacheResponse> DeleteCacheAsync(string cacheName)
    {
        return await this.simpleCacheClient.DeleteCacheAsync(cacheName);
    }

    /// <inheritdoc />
    public async Task<ListCachesResponse> ListCachesAsync(string? nextPageToken = null)
    {
        return await this.simpleCacheClient.ListCachesAsync(nextPageToken);
    }

    /// <inheritdoc />
    public async Task<CacheSetResponse> SetAsync(string cacheName, byte[] key, byte[] value, TimeSpan? ttl = null)
    {
        return await this.simpleCacheClient.SetAsync(cacheName, key, value, ttl);
    }

    /// <inheritdoc />
    public async Task<CacheGetResponse> GetAsync(string cacheName, byte[] key)
    {
        return await this.simpleCacheClient.GetAsync(cacheName, key);
    }

    /// <inheritdoc />
    public async Task<CacheDeleteResponse> DeleteAsync(string cacheName, byte[] key)
    {
        return await this.simpleCacheClient.DeleteAsync(cacheName, key);
    }

    /// <inheritdoc />
    public async Task<CacheSetResponse> SetAsync(string cacheName, string key, string value, TimeSpan? ttl = null)
    {
        return await simpleCacheClient.SetAsync(cacheName, key, value, ttl);
    }

    /// <inheritdoc />
    public async Task<CacheGetResponse> GetAsync(string cacheName, string key)
    {
        return await this.simpleCacheClient.GetAsync(cacheName, key);
    }

    /// <inheritdoc />
    public async Task<CacheDeleteResponse> DeleteAsync(string cacheName, string key)
    {
        return await this.simpleCacheClient.DeleteAsync(cacheName, key);
    }

    /// <inheritdoc />
    public async Task<CacheSetResponse> SetAsync(string cacheName, string key, byte[] value, TimeSpan? ttl = null)
    {
        return await this.simpleCacheClient.SetAsync(cacheName, key, value, ttl);
    }

    /// <summary>
    /// Gets multiple values from the cache.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="keys">The keys to get.</param>
    /// <returns>Task object representing the statuses of the get operation and the associated values.</returns>
    public async Task<CacheGetBatchResponse> GetBatchAsync(string cacheName, IEnumerable<byte[]> keys)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(keys, nameof(keys));
            Utils.ElementsNotNull(keys, nameof(keys));
        }
        catch (ArgumentNullException e)
        {
            return new CacheGetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.GetBatchAsync(this, cacheName, keys);
    }

    /// <inheritdoc cref="GetBatchAsync(string, IEnumerable{byte[]})"/>
    public async Task<CacheGetBatchResponse> GetBatchAsync(string cacheName, IEnumerable<string> keys)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(keys, nameof(keys));
            Utils.ElementsNotNull(keys, nameof(keys));
        }
        catch (ArgumentNullException e)
        {
            return new CacheGetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.GetBatchAsync(this, cacheName, keys);
    }

    /// <summary>
    /// Sets multiple items in the cache. Overwrites existing items.
    /// </summary>
    /// <param name="cacheName">Name of the cache to store the items in.</param>
    /// <param name="items">The items to set.</param>
    /// <param name="ttl">TTL for the item in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <returns>Task object representing the result of the set operation.</returns>
    public async Task<CacheSetBatchResponse> SetBatchAsync(string cacheName, IEnumerable<KeyValuePair<byte[], byte[]>> items, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(items, nameof(items));
            Utils.KeysAndValuesNotNull(items, nameof(items));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.SetBatchAsync(this, cacheName, items, ttl);
    }

    /// <inheritdoc cref="SetBatchAsync(string, IEnumerable{KeyValuePair{byte[], byte[]}}, TimeSpan?)"/>
    public async Task<CacheSetBatchResponse> SetBatchAsync(string cacheName, IEnumerable<KeyValuePair<string, string>> items, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(items, nameof(items));
            Utils.KeysAndValuesNotNull(items, nameof(items));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.SetBatchAsync(this, cacheName, items, ttl);
    }

    /// <summary>
    /// Set the dictionary field to a value with a given time to live (TTL) seconds.
    /// </summary>
    /// <remark>
    /// Creates the data structure if it does not exist and sets the TTL.
    /// If the data structure already exists and <paramref name="refreshTtl"/> is <see langword="true"/>,
    /// then update the TTL to <paramref name="ttl"/>, otherwise leave the TTL unchanged.
    /// </remark>
    /// <param name="cacheName">Name of the cache to store the dictionary in.</param>
    /// <param name="dictionaryName">The dictionary to set.</param>
    /// <param name="field">The field in the dictionary to set.</param>
    /// <param name="value">The value to be stored.</param>
    /// <param name="refreshTtl">Update the dictionary TTL if the dictionary already exists.</param>
    /// <param name="ttl">TTL for the dictionary in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheDictionarySetResponse> DictionarySetAsync(string cacheName, string dictionaryName, byte[] field, byte[] value, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
            Utils.ArgumentNotNull(value, nameof(value));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionarySetResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionarySetAsync(cacheName, dictionaryName, field, value, refreshTtl, ttl);
    }

    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)"/>
    public async Task<CacheDictionarySetResponse> DictionarySetAsync(string cacheName, string dictionaryName, string field, string value, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
            Utils.ArgumentNotNull(value, nameof(value));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionarySetResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionarySetAsync(cacheName, dictionaryName, field, value, refreshTtl, ttl);
    }

    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)"/>
    public async Task<CacheDictionarySetResponse> DictionarySetAsync(string cacheName, string dictionaryName, string field, byte[] value, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
            Utils.ArgumentNotNull(value, nameof(value));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionarySetResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionarySetAsync(cacheName, dictionaryName, field, value, refreshTtl, ttl);
    }

    /// <summary>
    /// Get the cache value stored for the given dictionary and field.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="dictionaryName">The dictionary to lookup.</param>
    /// <param name="field">The field in the dictionary to lookup.</param>
    /// <returns>Task representing the status of the get operation and the associated value.</returns>
    public async Task<CacheDictionaryGetResponse> DictionaryGetAsync(string cacheName, string dictionaryName, byte[] field)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryGetResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionaryGetAsync(cacheName, dictionaryName, field);
    }

    /// <inheritdoc cref="DictionaryGetAsync(string, string, byte[])"/>
    public async Task<CacheDictionaryGetResponse> DictionaryGetAsync(string cacheName, string dictionaryName, string field)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryGetResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionaryGetAsync(cacheName, dictionaryName, field);
    }

    /// <summary>
    /// Set several dictionary field-value pairs in the cache.
    /// </summary>
    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)" path="remark"/>
    /// <param name="cacheName">Name of the cache to store the dictionary in.</param>
    /// <param name="dictionaryName">The dictionary to set.</param>
    /// <param name="items">The field-value pairs in the dictionary to set.</param>
    /// <param name="refreshTtl">Update the dictionary TTL if the dictionary already exists.</param>
    /// <param name="ttl">TTL for the dictionary in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheDictionarySetBatchResponse> DictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<byte[], byte[]>> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(items, nameof(items));
            Utils.KeysAndValuesNotNull(items, nameof(items));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionarySetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionarySetBatchAsync(cacheName, dictionaryName, items, refreshTtl, ttl);
    }

    /// <inheritdoc cref="DictionarySetBatchAsync(string, string, IEnumerable{KeyValuePair{byte[], byte[]}}, bool, TimeSpan?)"/>
    public async Task<CacheDictionarySetBatchResponse> DictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<string, string>> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(items, nameof(items));
            Utils.KeysAndValuesNotNull(items, nameof(items));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionarySetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionarySetBatchAsync(cacheName, dictionaryName, items, refreshTtl, ttl);
    }

    /// <inheritdoc cref="DictionarySetBatchAsync(string, string, IEnumerable{KeyValuePair{byte[], byte[]}}, bool, TimeSpan?)"/>
    public async Task<CacheDictionarySetBatchResponse> DictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<string, byte[]>> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(items, nameof(items));
            Utils.KeysAndValuesNotNull(items, nameof(items));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionarySetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionarySetBatchAsync(cacheName, dictionaryName, items, refreshTtl, ttl);
    }

    /// <summary>
    /// <para>Add an integer quantity to a dictionary value.</para>
    ///
    /// <para>Incrementing the value of a missing field sets the value to <paramref name="amount"/>.</para>
    /// <para>Incrementing a value that was not set using this method or not the string representation of an integer
    /// results in an error with <see cref="FailedPreconditionException"/>.</para>
    /// </summary>
    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)" path="remark"/>
    /// <param name="cacheName">Name of the cache to store the dictionary in.</param>
    /// <param name="dictionaryName">The dictionary to set.</param>
    /// <param name="field"></param>
    /// <param name="refreshTtl">Update the dictionary TTL if the dictionary already exists.</param>
    /// <param name="amount">The quantity to add to the value. May be positive, negative, or zero. Defaults to 1.</param>
    /// <param name="ttl">TTL for the dictionary in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    /// <example>
    /// The following illustrates a typical workflow:
    /// <code>
    ///     var response = await client.DictionaryIncrementAsync(cacheName, "my dictionary", "counter", amount: 42, refreshTtl: false);
    ///     if (response is CacheDictionaryIncrementResponse.Success success)
    ///     {
    ///         Console.WriteLine($"Current value is {success.Value}");
    ///     }
    ///     else if (response is CacheDictionaryIncrementResponse.Error error)
    ///     {
    ///         Console.WriteLine($"Got an error: {error.Message}");
    ///     }
    ///
    ///     // Reset the counter. Note we use the string representation of an integer.
    ///     var setResponse = await client.DictionarySetAsync(cacheName, "my dictionary", "counter", "0", refreshTtl: false);
    ///     if (setResponse is CacheDictionarySetResponse.Error) { /* handle error */ }
    ///
    ///     // Retrieve the counter. The integer is represented as a string.
    ///     var getResponse = await client.DictionaryGetAsync(cacheName, "my dictionary", "counter");
    ///     if (getResponse is CacheDictionaryGetResponse.Hit getHit)
    ///     {
    ///         Console.WriteLine(getHit.String());
    ///     }
    ///     else if (getResponse is CacheDictionaryGetResponse.Error) { /* handle error */ }
    ///
    ///     // Here we try incrementing a value that isn't an integer. This results in an error with <see cref="FailedPreconditionException"/>
    ///     setResponse = await client.DictionarySetAsync(cacheName, "my dictionary", "counter", "0123ABC", refreshTtl: false);
    ///     if (setResponse is CacheDictionarySetResponse.Error) { /* handle error */ }
    ///
    ///     var incrementResponse = await client.DictionaryIncrementAsync(cacheName, "my dictionary", "counter", amount: 42, refreshTtl: false);
    ///     if (incrementResponse is CacheDictionaryIncrementResponse.Error badIncrement)
    ///     {
    ///         Console.WriteLine($"Could not increment dictionary field: {badIncrement.Message}");
    ///     }
    /// </code>
    /// </example>
    public async Task<CacheDictionaryIncrementResponse> DictionaryIncrementAsync(string cacheName, string dictionaryName, string field, bool refreshTtl, long amount = 1, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryIncrementResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionaryIncrementAsync(cacheName, dictionaryName, field, refreshTtl, amount, ttl);
    }

    /// <summary>
    /// Get several values from a dictionary.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="dictionaryName">The dictionary to lookup.</param>
    /// <param name="fields">The fields in the dictionary to lookup.</param>
    /// <returns>Task representing the status and associated value for each field.</returns>
    public async Task<CacheDictionaryGetBatchResponse> DictionaryGetBatchAsync(string cacheName, string dictionaryName, IEnumerable<byte[]> fields)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(fields, nameof(fields));
            Utils.ElementsNotNull(fields, nameof(fields));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryGetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.DictionaryGetBatchAsync(cacheName, dictionaryName, fields);
    }

    /// <inheritdoc cref="DictionaryGetBatchAsync(string, string, IEnumerable{byte[]})"/>
    public async Task<CacheDictionaryGetBatchResponse> DictionaryGetBatchAsync(string cacheName, string dictionaryName, IEnumerable<string> fields)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(fields, nameof(fields));
            Utils.ElementsNotNull(fields, nameof(fields));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryGetBatchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionaryGetBatchAsync(cacheName, dictionaryName, fields);
    }

    /// <summary>
    /// Fetch the entire dictionary from the cache.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="dictionaryName">The dictionary to fetch.</param>
    /// <returns>Task representing with the status of the fetch operation and the associated dictionary.</returns>
    public async Task<CacheDictionaryFetchResponse> DictionaryFetchAsync(string cacheName, string dictionaryName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryFetchResponse.Error(new InvalidArgumentException(e.Message));
        }


        return await this.dataClient.DictionaryFetchAsync(cacheName, dictionaryName);
    }

    /// <summary>
    /// Remove the dictionary from the cache.
    ///
    /// Performs a no-op if <paramref name="dictionaryName"/> does not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to delete the dictionary from.</param>
    /// <param name="dictionaryName">Name of the dictionary to delete.</param>
    /// <returns>Task representing the result of the delete operation.</returns>
    public async Task<CacheDictionaryDeleteResponse> DictionaryDeleteAsync(string cacheName, string dictionaryName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheDictionaryDeleteResponse.Error(new InvalidArgumentException(e.Message));
        }


        return await this.dataClient.DictionaryDeleteAsync(cacheName, dictionaryName);
    }

    /// <summary>
    /// Remove a field from a dictionary.
    ///
    /// Performs a no-op if <paramref name="dictionaryName"/> or <paramref name="field"/> does not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to lookup the dictionary in.</param>
    /// <param name="dictionaryName">Name of the dictionary to remove the field from.</param>
    /// <param name="field">Name of the field to remove from the dictionary.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, byte[] field)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
        }

        catch (ArgumentNullException e)
        {
            return new CacheDictionaryRemoveFieldResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field);
    }

    /// <inheritdoc cref="DictionaryRemoveFieldAsync(string, string, byte[])"/>
    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, string field)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(field, nameof(field));
        }

        catch (ArgumentNullException e)
        {
            return new CacheDictionaryRemoveFieldResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field);
    }

    /// <summary>
    /// Remove fields from a dictionary.
    ///
    /// Performs a no-op if <paramref name="dictionaryName"/> or a particular field does not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to lookup the dictionary in.</param>
    /// <param name="dictionaryName">Name of the dictionary to remove the field from.</param>
    /// <param name="fields">Name of the fields to remove from the dictionary.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<byte[]> fields)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(fields, nameof(fields));
            Utils.ElementsNotNull(fields, nameof(fields));
        }

        catch (ArgumentNullException e)
        {
            return new CacheDictionaryRemoveFieldsResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fields);
    }

    /// <inheritdoc cref="DictionaryRemoveFieldsAsync(string, string, IEnumerable{byte[]})"/>
    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<string> fields)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(dictionaryName, nameof(dictionaryName));
            Utils.ArgumentNotNull(fields, nameof(fields));
            Utils.ElementsNotNull(fields, nameof(fields));
        }

        catch (ArgumentNullException e)
        {
            return new CacheDictionaryRemoveFieldsResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fields);
    }

    /// <summary>
    /// Add an element to a set in the cache.
    ///
    /// After this operation, the set will contain the union
    /// of the element passed in and the elements of the set.
    /// </summary>
    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)" path="remark"/>
    /// <param name="cacheName">Name of the cache to store the set in.</param>
    /// <param name="setName">The set to add the element to.</param>
    /// <param name="element">The data to add to the set.</param>
    /// <param name="refreshTtl">Update <paramref name="setName"/>'s TTL if it already exists.</param>
    /// <param name="ttl">TTL for the set in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheSetAddResponse> SetAddAsync(string cacheName, string setName, byte[] element, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(element, nameof(element));
        }

        catch (ArgumentNullException e)
        {
            return new CacheSetAddResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.SetAddAsync(cacheName, setName, element, refreshTtl, ttl);
    }

    /// <inheritdoc cref="SetAddAsync(string, string, byte[], bool, TimeSpan?)"/>
    public async Task<CacheSetAddResponse> SetAddAsync(string cacheName, string setName, string element, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(element, nameof(element));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetAddResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetAddAsync(cacheName, setName, element, refreshTtl, ttl);
    }

    /// <summary>
    /// Add several elements to a set in the cache.
    ///
    /// After this operation, the set will contain the union
    /// of the elements passed in and the elements of the set.
    /// </summary>
    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)" path="remark"/>
    /// <param name="cacheName">Name of the cache to store the set in.</param>
    /// <param name="setName">The set to add elements to.</param>
    /// <param name="elements">The data to add to the set.</param>
    /// <param name="refreshTtl">Update <paramref name="setName"/>'s TTL if it already exists.</param>
    /// <param name="ttl">TTL for the set in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheSetAddBatchResponse> SetAddBatchAsync(string cacheName, string setName, IEnumerable<byte[]> elements, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(elements, nameof(elements));
            Utils.ElementsNotNull(elements, nameof(elements));
        }

        catch (ArgumentNullException e)
        {
            return new CacheSetAddBatchResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.SetAddBatchAsync(cacheName, setName, elements, refreshTtl, ttl);
    }

    /// <inheritdoc cref="SetAddBatchAsync(string, string, IEnumerable{byte[]}, bool, TimeSpan?)"/>
    public async Task<CacheSetAddBatchResponse> SetAddBatchAsync(string cacheName, string setName, IEnumerable<string> elements, bool refreshTtl, TimeSpan? ttl = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(elements, nameof(elements));
            Utils.ElementsNotNull(elements, nameof(elements));
        }

        catch (ArgumentNullException e)
        {
            return new CacheSetAddBatchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetAddBatchAsync(cacheName, setName, elements, refreshTtl, ttl);
    }

    /// <summary>
    /// Remove an element from a set.
    ///
    /// Performs a no-op if <paramref name="setName"/> or <paramref name="element"/> does not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to lookup the set in.</param>
    /// <param name="setName">The set to remove the element from.</param>
    /// <param name="element">The data to remove from the set.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheSetRemoveElementResponse> SetRemoveElementAsync(string cacheName, string setName, byte[] element)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(element, nameof(element));
        }

        catch (ArgumentNullException e)
        {
            return new CacheSetRemoveElementResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.SetRemoveElementAsync(cacheName, setName, element);
    }

    /// <inheritdoc cref="SetRemoveElementAsync(string, string, byte[])"/>
    public async Task<CacheSetRemoveElementResponse> SetRemoveElementAsync(string cacheName, string setName, string element)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(element, nameof(element));
        }

        catch (ArgumentNullException e)
        {
            return new CacheSetRemoveElementResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetRemoveElementAsync(cacheName, setName, element);
    }

    /// <summary>
    /// Remove elements from a set.
    ///
    /// Performs a no-op if <paramref name="setName"/> or any of <paramref name="elements"/> do not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to lookup the set in.</param>
    /// <param name="setName">The set to remove the elements from.</param>
    /// <param name="elements">The data to remove from the set.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheSetRemoveElementsResponse> SetRemoveElementsAsync(string cacheName, string setName, IEnumerable<byte[]> elements)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(elements, nameof(elements));
            Utils.ElementsNotNull(elements, nameof(elements));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetRemoveElementsResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetRemoveElementsAsync(cacheName, setName, elements);
    }

    /// <inheritdoc cref="SetRemoveElementsAsync(string, string, IEnumerable{byte[]})"/>
    public async Task<CacheSetRemoveElementsResponse> SetRemoveElementsAsync(string cacheName, string setName, IEnumerable<string> elements)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
            Utils.ArgumentNotNull(elements, nameof(elements));
            Utils.ElementsNotNull(elements, nameof(elements));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetRemoveElementsResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetRemoveElementsAsync(cacheName, setName, elements);
    }

    /// <summary>
    /// Fetch the entire set from the cache.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="setName">The set to fetch.</param>
    /// <returns>Task representing with the status of the fetch operation and the associated set.</returns>
    public async Task<CacheSetFetchResponse> SetFetchAsync(string cacheName, string setName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetFetchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetFetchAsync(cacheName, setName);
    }

    /// <summary>
    /// Remove the set from the cache.
    ///
    /// Performs a no-op if <paramref name="setName"/> does not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to delete the set from.</param>
    /// <param name="setName">Name of the set to delete.</param>
    /// <returns>Task representing the result of the delete operation.</returns>
    public async Task<CacheSetDeleteResponse> SetDeleteAsync(string cacheName, string setName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(setName, nameof(setName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheSetDeleteResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.SetDeleteAsync(cacheName, setName);
    }

    /// <summary>
    /// Adds multiple values to the beginning of a list in the exact order given.
    /// </summary>
    /// <param name="cacheName">Name of the cache to store the list in.</param>
    /// <param name="listName">The list to add the value on.</param>
    /// <param name="values">The values to add to the front of the list.</param>
    /// <param name="refreshTtl">Update <paramref name="listName"/>'s TTL if it already exists.</param>
    /// <param name="ttl">TTL for the list in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <param name="truncateBackToSize">Ensure the list does not exceed this length. Remove excess from the end of the list. Must be a positive number.</param>
    /// <returns>Task representing the result of the push operation.</returns>
    public async Task<CacheListConcatenateFrontResponse> ListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<byte[]> values, bool refreshTtl, TimeSpan? ttl = null, int? truncateBackToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(values, nameof(values));
            Utils.ElementsNotNull(values, nameof(values));
            Utils.ArgumentStrictlyPositive(truncateBackToSize, nameof(truncateBackToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListConcatenateFrontResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListConcatenateFrontResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListConcatenateFrontAsync(cacheName, listName, values, refreshTtl, truncateBackToSize, ttl);
    }

    /// <inheritdoc cref="ListConcatenateFrontAsync(string, string, IEnumerable{byte[]}, bool, TimeSpan?, int?)"/>
    public async Task<CacheListConcatenateFrontResponse> ListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<string> values, bool refreshTtl, TimeSpan? ttl = null, int? truncateBackToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(values, nameof(values));
            Utils.ElementsNotNull(values, nameof(values));
            Utils.ArgumentStrictlyPositive(truncateBackToSize, nameof(truncateBackToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListConcatenateFrontResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListConcatenateFrontResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListConcatenateFrontAsync(cacheName, listName, values, refreshTtl, truncateBackToSize, ttl);
    }

    /// <summary>
    /// Adds multiple values to the back of a list in the exact order given.
    /// </summary>
    /// <param name="cacheName">Name of the cache to store the list in.</param>
    /// <param name="listName">The list to add the value on.</param>
    /// <param name="values">The values to add to the back of the list.</param>
    /// <param name="refreshTtl">Update <paramref name="listName"/>'s TTL if it already exists.</param>
    /// <param name="ttl">TTL for the list in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <param name="truncateFrontToSize">Ensure the list does not exceed this length. Remove excess from the front of the list. Must be a positive number.</param>
    /// <returns>Task representing the result of the push operation.</returns>
    public async Task<CacheListConcatenateBackResponse> ListConcatenateBackAsync(string cacheName, string listName, IEnumerable<byte[]> values, bool refreshTtl, TimeSpan? ttl = null, int? truncateFrontToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(values, nameof(values));
            Utils.ElementsNotNull(values, nameof(values));
            Utils.ArgumentStrictlyPositive(truncateFrontToSize, nameof(truncateFrontToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListConcatenateBackResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListConcatenateBackResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListConcatenateBackAsync(cacheName, listName, values, refreshTtl, truncateFrontToSize, ttl);
    }

    /// <inheritdoc cref="ListConcatenateBackAsync(string, string, IEnumerable{byte[]}, bool, TimeSpan?, int?)"/>
    public async Task<CacheListConcatenateBackResponse> ListConcatenateBackAsync(string cacheName, string listName, IEnumerable<string> values, bool refreshTtl, TimeSpan? ttl = null, int? truncateFrontToSize = null)
    {
         try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(values, nameof(values));
            Utils.ElementsNotNull(values, nameof(values));
            Utils.ArgumentStrictlyPositive(truncateFrontToSize, nameof(truncateFrontToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListConcatenateBackResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListConcatenateBackResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListConcatenateBackAsync(cacheName, listName, values, refreshTtl, truncateFrontToSize, ttl);
    }


    /// <summary>
    /// Push a value to the beginning of a list.
    /// </summary>
    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)" path="remark"/>
    /// <param name="cacheName">Name of the cache to store the list in.</param>
    /// <param name="listName">The list to push the value on.</param>
    /// <param name="value">The value to push to the front of the list.</param>
    /// <param name="refreshTtl">Update <paramref name="listName"/>'s TTL if it already exists.</param>
    /// <param name="ttl">TTL for the list in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <param name="truncateBackToSize">Ensure the list does not exceed this length. Remove excess from the end of the list. Must be a positive number.</param>
    /// <returns>Task representing the result of the push operation.</returns>
    public async Task<CacheListPushFrontResponse> ListPushFrontAsync(string cacheName, string listName, byte[] value, bool refreshTtl, TimeSpan? ttl = null, int? truncateBackToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(value, nameof(value));
            Utils.ArgumentStrictlyPositive(truncateBackToSize, nameof(truncateBackToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListPushFrontResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListPushFrontResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListPushFrontAsync(cacheName, listName, value, refreshTtl, truncateBackToSize, ttl);
    }

    /// <inheritdoc cref="ListPushFrontAsync(string, string, byte[], bool, TimeSpan?, int?)"/>
    public async Task<CacheListPushFrontResponse> ListPushFrontAsync(string cacheName, string listName, string value, bool refreshTtl, TimeSpan? ttl = null, int? truncateBackToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(value, nameof(value));
            Utils.ArgumentStrictlyPositive(truncateBackToSize, nameof(truncateBackToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListPushFrontResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListPushFrontResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListPushFrontAsync(cacheName, listName, value, refreshTtl, truncateBackToSize, ttl);
    } 

    /// <summary>
    /// Push a value to the end of a list.
    /// </summary>
    /// <inheritdoc cref="DictionarySetAsync(string, string, byte[], byte[], bool, TimeSpan?)" path="remark"/>
    /// <param name="cacheName">Name of the cache to store the list in.</param>
    /// <param name="listName">The list to push the value on.</param>
    /// <param name="value">The value to push to the back of the list.</param>
    /// <param name="refreshTtl">Update <paramref name="listName"/>'s TTL if it already exists.</param>
    /// <param name="ttl">TTL for the list in cache. This TTL takes precedence over the TTL used when initializing a cache client. Defaults to client TTL.</param>
    /// <param name="truncateFrontToSize">Ensure the list does not exceed this length. Remove excess from the beginning of the list. Must be a positive number.</param>
    /// <returns>Task representing the result of the push operation.</returns>
    public async Task<CacheListPushBackResponse> ListPushBackAsync(string cacheName, string listName, byte[] value, bool refreshTtl, TimeSpan? ttl = null, int? truncateFrontToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(value, nameof(value));
            Utils.ArgumentStrictlyPositive(truncateFrontToSize, nameof(truncateFrontToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListPushBackResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListPushBackResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListPushBackAsync(cacheName, listName, value, refreshTtl, truncateFrontToSize, ttl);
    }

    /// <inheritdoc cref="ListPushBackAsync(string, string, byte[], bool, TimeSpan?, int?)"/>
    public async Task<CacheListPushBackResponse> ListPushBackAsync(string cacheName, string listName, string value, bool refreshTtl, TimeSpan? ttl = null, int? truncateFrontToSize = null)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(value, nameof(value));
            Utils.ArgumentStrictlyPositive(truncateFrontToSize, nameof(truncateFrontToSize));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListPushBackResponse.Error(new InvalidArgumentException(e.Message));
        }
        catch (ArgumentOutOfRangeException e)
        {
            return new CacheListPushBackResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListPushBackAsync(cacheName, listName, value, refreshTtl, truncateFrontToSize, ttl);
    }

    /// <summary>
    /// Retrieve and remove the first item from a list.
    /// </summary>
    /// <param name="cacheName">Name of the cache to read the list from.</param>
    /// <param name="listName">The list to pop from.</param>
    /// <returns>Task representing the status and associated value for the pop operation.</returns>
    public async Task<CacheListPopFrontResponse> ListPopFrontAsync(string cacheName, string listName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListPopFrontResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListPopFrontAsync(cacheName, listName);
    }

    /// <summary>
    /// Retrieve and remove the last item from a list.
    /// </summary>
    /// <param name="cacheName">Name of the cache to read the list from.</param>
    /// <param name="listName">The list to pop from.</param>
    /// <returns>Task representing the status and associated value for the pop operation.</returns>
    public async Task<CacheListPopBackResponse> ListPopBackAsync(string cacheName, string listName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListPopBackResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListPopBackAsync(cacheName, listName);
    }

    /// <summary>
    /// Fetch the entire list from the cache.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="listName">The list to fetch.</param>
    /// <returns>Task representing with the status of the fetch operation and the associated list.</returns>
    public async Task<CacheListFetchResponse> ListFetchAsync(string cacheName, string listName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListFetchResponse.Error(new InvalidArgumentException(e.Message));
        }

        return await this.dataClient.ListFetchAsync(cacheName, listName);
    }

    /// <summary>
    /// Remove all elements in a list equal to a particular value.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="listName">The list to remove elements from.</param>
    /// <param name="value">The value to completely remove from the list.</param>
    /// <returns>Task representing the result of the cache operation.</returns>
    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, byte[] value)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(value, nameof(value));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListRemoveValueResponse.Error(new InvalidArgumentException(e.Message));
        }


        return await this.dataClient.ListRemoveValueAsync(cacheName, listName, value);
    }

    /// <inheritdoc cref="ListRemoveValueAsync(string, string, byte[])"/>
    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, string value)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
            Utils.ArgumentNotNull(value, nameof(value));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListRemoveValueResponse.Error(new InvalidArgumentException(e.Message));
        }


        return await this.dataClient.ListRemoveValueAsync(cacheName, listName, value);
    }

    /// <summary>
    /// Calculate the length of a list in the cache.
    ///
    /// A list that does not exist is interpreted to have length 0.
    /// </summary>
    /// <param name="cacheName">Name of the cache to perform the lookup in.</param>
    /// <param name="listName">The list to calculate length.</param>
    /// <returns>Task representing the length of the list.</returns>
    public async Task<CacheListLengthResponse> ListLengthAsync(string cacheName, string listName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
        }

        catch (ArgumentNullException e)
        {
            return new CacheListLengthResponse.Error(new InvalidArgumentException(e.Message));
        }
        return await this.dataClient.ListLengthAsync(cacheName, listName);
    }

    /// <summary>
    /// Remove the list from the cache.
    ///
    /// Performs a no-op if <paramref name="listName"/> does not exist.
    /// </summary>
    /// <param name="cacheName">Name of the cache to delete the list from.</param>
    /// <param name="listName">Name of the list to delete.</param>
    /// <returns>Task representing the result of the delete operation.</returns>
    public async Task<CacheListDeleteResponse> ListDeleteAsync(string cacheName, string listName)
    {
        try
        {
            Utils.ArgumentNotNull(cacheName, nameof(cacheName));
            Utils.ArgumentNotNull(listName, nameof(listName));
        }
        catch (ArgumentNullException e)
        {
            return new CacheListDeleteResponse.Error(new InvalidArgumentException(e.Message));
        }


        return await this.dataClient.ListDeleteAsync(cacheName, listName);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        this.simpleCacheClient.Dispose();
        this.dataClient.Dispose();
    }
}
