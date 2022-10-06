using Microsoft.Extensions.Logging;
using Momento.Sdk.Config;
using Momento.Sdk.Incubating;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Responses;

public class Driver
{
    private static readonly string AUTH_TOKEN_ENV_VAR = "TEST_AUTH_TOKEN";
    private static readonly string CACHE_NAME_ENV_VAR = "TEST_CACHE_NAME";
    private static readonly ILogger _logger;
    private static readonly ILoggerFactory _loggerFactory;

    static Driver()
    {
        _loggerFactory = InitializeLogging();
        _logger = _loggerFactory.CreateLogger<Driver>();
    }

    public async static Task Main()
    {
        var authToken = ReadAuthToken();
        var cacheName = ReadCacheName();

        // Set up the client
        using var client = Momento.Sdk.Incubating.SimpleCacheClientFactory.CreateClient(Configurations.Laptop.Latest, authToken, 60, _loggerFactory);
        await EnsureCacheExistsAsync(client, cacheName);

        // Set a value
        var dictionaryName = "my-dictionary";
        var setResponse = await client.DictionarySetAsync(cacheName: cacheName, dictionaryName: dictionaryName,
            field: "my-field", value: "my-value", refreshTtl: false, ttlSeconds: 60);
        if (setResponse is CacheDictionarySetResponse.Error setError)
        {
            _logger.LogInformation($"Error setting a value in a dictionary: {setError.Message}");
            Environment.Exit(1);
        }

        // Set multiple values
        var setBatchResponse = await client.DictionarySetBatchAsync(
            cacheName: cacheName,
            dictionaryName: dictionaryName,
            new Dictionary<string, string>() {
                { "field1", "value1" },
                { "field2", "value2" },
                { "field3", "value3" }},
            refreshTtl: false);
        if (setBatchResponse is CacheDictionarySetBatchResponse.Error setBatchError)
        {
            _logger.LogInformation($"Error setting a values in a dictionary: {setBatchError.Message}");
            Environment.Exit(1);
        }

        // Get a value
        var field = "field1";
        var getResponse = await client.DictionaryGetAsync(
            cacheName: cacheName,
            dictionaryName: dictionaryName,
            field: field);

        var status = "";
        var value = "";
        if (getResponse is CacheDictionaryGetResponse.Hit unaryHit)
        {
            status = "HIT";
            value = unaryHit.String();
        }
        else if (getResponse is CacheDictionaryGetResponse.Miss)
        {
            // In this example you can get here if you:
            // - change the field name to one that does not exist, or if you
            // - set a short TTL, then add a Task.Delay so that it expires.
            status = "MISS";
            value = "<NONE; operation was a MISS>";
        }
        else if (getResponse is CacheDictionaryGetResponse.Error getError)
        {
            _logger.LogInformation($"Error getting value from a dictionary: {getError.Message}");
            Environment.Exit(1);
        }

        _logger.LogInformation("");
        _logger.LogInformation($"Dictionary get of {field}: status={status}; value={value}");

        // Get multiple values
        var batchFieldList = new string[] { "field1", "field2", "field3", "field4" };
        var getBatchResponse = await client.DictionaryGetBatchAsync(
            cacheName: cacheName,
            dictionaryName: dictionaryName,
            fields: batchFieldList);
        if (getBatchResponse is CacheDictionaryGetBatchResponse.Success responses)
        {
            _logger.LogInformation("");
            _logger.LogInformation("Displaying the result of dictionary get batch:");
            foreach ((var batchField, var response) in batchFieldList.Zip(responses.Responses))
            {
                status = "MISS";
                value = "<NONE; field was a MISS>";
                if (response is CacheDictionaryGetResponse.Hit hit_)
                {
                    status = "HIT";
                    value = hit_.String();
                }
                _logger.LogInformation($"- field={batchField}; status={status}; value={value}");
            }
        }
        else if (getBatchResponse is CacheDictionaryGetBatchResponse.Error getBatchError)
        {
            _logger.LogInformation($"Error getting value from a dictionary: {getBatchError.Message}");
            Environment.Exit(1);
        }

        // Get the whole dictionary
        var fetchResponse = await client.DictionaryFetchAsync(
            cacheName: cacheName,
            dictionaryName: dictionaryName);
        if (fetchResponse is CacheDictionaryFetchResponse.Hit fetchHit)
        {
            var dictionary = fetchHit.StringStringDictionary();
            _logger.LogInformation("");
            _logger.LogInformation($"Accessing an entry of {dictionaryName} using a native Dictionary: {dictionary["field1"]}");

            _logger.LogInformation("");
            _logger.LogInformation("Displaying the results of dictionary fetch:");
            dictionary.ToList().ForEach(kv =>
                _logger.LogInformation($"- field={kv.Key}; value={kv.Value}"));
        }
        else if (fetchResponse is CacheDictionaryFetchResponse.Miss fetchMiss)
        {
            // You can reach here by:
            // - fetching a dictionary that does not exist, eg changing the name above, or
            // - setting a short TTL and adding a Task.Delay so the dictionary expires
            _logger.LogInformation($"Expected {dictionaryName} to be a hit; got a miss.");
            Environment.Exit(1);
        }
        else if (fetchResponse is CacheDictionaryFetchResponse.Error fetchError)
        {
            _logger.LogInformation($"Error while fetching {dictionaryName}: {fetchError.Message}");
            Environment.Exit(1);
        }
    }

    private static ILoggerFactory InitializeLogging()
    {
        return LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.SingleLine = true;
                options.TimestampFormat = "hh:mm:ss ";
            });
            builder.SetMinimumLevel(LogLevel.Information);
        });
    }

    private static string ReadAuthToken()
    {
        var authToken = System.Environment.GetEnvironmentVariable(AUTH_TOKEN_ENV_VAR);
        if (authToken is null)
        {
            Console.Write($"Auth token not detected in environment variable {AUTH_TOKEN_ENV_VAR}. Enter auth token here: ");
            authToken = Console.ReadLine()!.Trim();
        }
        return authToken;
    }

    private static string ReadCacheName()
    {
        var cacheName = System.Environment.GetEnvironmentVariable(CACHE_NAME_ENV_VAR);
        if (cacheName is null)
        {
            Console.Write($"Cache name not detected in environment variable {CACHE_NAME_ENV_VAR}. Enter cache name here: ");
            cacheName = Console.ReadLine()!.Trim();
        }
        return cacheName;
    }

    private static async Task EnsureCacheExistsAsync(SimpleCacheClient client, string cacheName)
    {
        _logger.LogInformation($"Creating cache {cacheName} if it doesn't already exist.");
        var createCacheResponse = await client.CreateCacheAsync(cacheName);
        if (createCacheResponse is CreateCacheResponse.Success)
        {
            _logger.LogInformation($"Created cache {cacheName}.");
        }
        else if (createCacheResponse is CreateCacheResponse.CacheAlreadyExists)
        {
            _logger.LogInformation($"Cache {cacheName} already exists.");
        }
        else if (createCacheResponse is CreateCacheResponse.Error error)
        {
            _logger.LogInformation($"Error creating cache: {error.Message}");
            Environment.Exit(1);
        }
    }
}
