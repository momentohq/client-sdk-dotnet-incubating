using Momento.Sdk.Exceptions;
using Momento.Sdk.Incubating;

public class Driver
{
    private static readonly string AUTH_TOKEN_ENV_VAR = "TEST_AUTH_TOKEN";
    private static readonly string CACHE_NAME_ENV_VAR = "TEST_CACHE_NAME";

    public async static Task Main()
    {
        var authToken = ReadAuthToken();
        var cacheName = ReadCacheName();

        using var client = Momento.Sdk.Incubating.SimpleCacheClientFactory.CreateClient(authToken, 60);
        EnsureCacheExists(client, cacheName);

        // Set a value
        await client.DictionarySetAsync(cacheName: cacheName, dictionaryName: "my-dictionary",
            field: "my-field", value: "my-value", refreshTtl: false, ttlSeconds: 60);

        // Set multiple values
        await client.DictionarySetBatchAsync(
            cacheName: cacheName,
            dictionaryName: "my-dictionary",
            new Dictionary<string, string>() {
                { "field1", "value1" },
                { "field2", "value2" },
                { "field3", "value3" }},
            refreshTtl: false);

        // Get a value
        var field = "field1";
        var getResponse = await client.DictionaryGetAsync(
            cacheName: cacheName,
            dictionaryName: "my-dictionary",
            field: field);
        var status = getResponse.Status; // HIT
        string value = getResponse.String()!; // "value1"
        Console.WriteLine($"Dictionary get of {field}: status={status}; value={value}");

        // Get multiple values
        var batchFieldList = new string[] { "field1", "field2", "field3", "field4" };
        var getBatchResponse = await client.DictionaryGetBatchAsync(
            cacheName: cacheName,
            dictionaryName: "my-dictionary",
            fields: batchFieldList);
        var manyStatus = getBatchResponse.Status; // [HIT, HIT, HIT, MISS]
        var values = getBatchResponse.Strings(); // ["value1", "value2", "value3", null]

        Console.WriteLine("\nDisplaying the result of dictionary get batch:");
        foreach ((var batchField, var response) in batchFieldList.Zip(getBatchResponse.Responses))
        {
            Console.WriteLine($"- field={batchField}; status={response.Status}; value={response.String()}");
        }

        // Get the whole dictionary
        var fetchResponse = await client.DictionaryFetchAsync(
            cacheName: cacheName,
            dictionaryName: "my-dictionary");
        status = fetchResponse.Status;
        var dictionary = fetchResponse.StringStringDictionary()!;
        value = dictionary["field1"]; // == "value1"

        Console.WriteLine("\nDisplaying the results of dictionary fetch:");
        dictionary.ToList().ForEach(kv =>
            Console.WriteLine($"- field={kv.Key}; value={kv.Value}"));
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

    private static void EnsureCacheExists(SimpleCacheClient client, string cacheName)
    {
        try
        {
            client.CreateCache(cacheName);
            Console.WriteLine($"Created cache {cacheName}");
        }
        catch (AlreadyExistsException)
        {
        }
    }
}
