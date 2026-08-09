using Azure.Monitor.OpenTelemetry.Exporter;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Azure.Functions.Worker.OpenTelemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry;
using Azure.Storage.Queues;
using Azure.Messaging.ServiceBus;


var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services.AddSingleton(sp =>
{
    var connectionString = builder.Configuration["CosmosDbConnectionString"];
    return new CosmosClient(connectionString);
});

builder.Services.AddSingleton(sp =>
{
    var connectionString = builder.Configuration["AzureWebJobsStorage"];
    return new QueueServiceClient(connectionString); 
});

builder.Services.AddSingleton(sp =>
{
    var connectionString = builder.Configuration["ServiceBusConnection"];
    return new ServiceBusClient(connectionString);
});

if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APPLICATIONINSIGHTS_CONNECTION_STRING")))
{
    builder.Services.AddOpenTelemetry()
        .UseFunctionsWorkerDefaults()
        .UseAzureMonitorExporter();

}

builder.Build().Run();