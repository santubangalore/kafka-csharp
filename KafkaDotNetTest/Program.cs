using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaDotNetTest
{
    class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, collection) =>
            {
                collection.AddHostedService<KafkaProducerHostedService>();
            });
    }

    public class KafkaProducerHostedService : IHostedService
    {
        public readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            this._logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();

        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
           for(int i = 0; i < 10; i++)
            {
                var value = $"Hello World {i}";
                this._logger.LogInformation(value);
                await this._producer.ProduceAsync("test", new Message<Null, string>()
                {
                    Value = value
                },cancellationToken) ;
            }

           this._producer.Flush(TimeSpan.FromSeconds(5));
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
           this._producer?.Dispose();
           return Task.CompletedTask;
        }
    }
}
