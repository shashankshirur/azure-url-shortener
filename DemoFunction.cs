using Azure.Messaging.ServiceBus;
using Azure.Storage.Queues;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Urlshorten.Function;

public class ShortenUrlFunction
{
    private readonly ILogger<ShortenUrlFunction> _logger;
    private readonly Container _container;

    private readonly QueueClient _queueClient;

    private readonly ServiceBusSender _serviceBusSender;

    public ShortenUrlFunction(ILogger<ShortenUrlFunction> logger, CosmosClient cosmosClient, QueueServiceClient queueServiceClient, ServiceBusClient serviceBusClient, IConfiguration configuration)
    {
        _logger = logger;
        var databaseName = configuration["CosmosDbDatabaseName"];
        var containerName = configuration["CosmosDbContainerName"];
        _container = cosmosClient.GetContainer(databaseName, containerName);
        _queueClient = queueServiceClient.GetQueueClient("link-clicks");
        _serviceBusSender = serviceBusClient.CreateSender("link-clicks");
    }

    public class ShortenRequest
    {
        public string LongUrl { get; set; } = string.Empty;
    }

    public class LinkDocument
    {
        [JsonProperty("id")]
        public string Id { get; set; } = string.Empty;
        [JsonProperty("code")]
        public string Code { get; set; } = string.Empty;
        public string LongUrl { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        
        [JsonProperty("clicks")]
        public int Clicks { get; set; }
    }

    [Function("ShortenUrl")]
    public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = "shorten")] HttpRequest req)
    {
        var body = await new StreamReader(req.Body).ReadToEndAsync();
        var request = System.Text.Json.JsonSerializer.Deserialize<ShortenRequest>(body, new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (request is null || string.IsNullOrWhiteSpace(request.LongUrl))
        {
            return new BadRequestObjectResult("Request body must include a 'longUrl' field.");
        }

        var code = Guid.NewGuid().ToString("N")[..6];
        var document = new LinkDocument()
        {
            Id = code,
            Code = code,
            LongUrl = request.LongUrl,
            CreatedAt = DateTime.UtcNow,
            Clicks = 0
        };
        await _container.CreateItemAsync(document, new PartitionKey(document.Code));
        _logger.LogInformation($"Created short link {code} ->  {request.LongUrl}");
        return new OkObjectResult(new { code, shortPath = $"/api/{code}" });
    }

    [Function("RedirectToUrl")]
    public async Task<IActionResult> Redirect([HttpTrigger(AuthorizationLevel.Function, "get", Route = "{code}")] HttpRequest req, string code)
    {
        try
        {
            var response = await _container.ReadItemAsync<LinkDocument>(code, new PartitionKey(code));
            
            //await _queueClient.SendMessageAsync(code);
            await _serviceBusSender.SendMessageAsync(new ServiceBusMessage(code));

            return new RedirectResult(response.Resource.LongUrl, permanent: false);
        }

        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return new NotFoundObjectResult($"No link found for code '{code}'.");
        }
    }

    [Function("IncrementClickCount")]
    public async Task IncrementClickCount([QueueTrigger("link-clicks", Connection = "AzureWebJobsStorage")] string code)
    {
        var patchOperations = new[] { PatchOperation.Increment("/clicks", 1)};
        await _container.PatchItemAsync<LinkDocument>(code, new PartitionKey(code), patchOperations);
        _logger.LogInformation($"Incremented click count for {code}");
    }

    [Function("IncrementClickCountServiceBus")]
    public async Task IncrementClickCountServiceBus([ServiceBusTrigger("link-clicks", Connection = "ServiceBusConnection")] string code)
    {
        // var patchOperations = new[] { PatchOperation.Increment("/clicks", 1)};
        // await _container.PatchItemAsync<LinkDocument>(code, new PartitionKey(code), patchOperations);
        // _logger.LogInformation($"Incremented click count for {code}");
        _logger.LogInformation("Testing DLQ");
        throw new InvalidOperationException("Testing DLQ");
    }
}