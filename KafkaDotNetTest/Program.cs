using Confluent.Kafka;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

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
                collection.AddHostedService<KafkaConsumerHostedService>();
                collection.AddHostedService<KafkaProducerHostedService>();
            });
    }

    /// <summary>
    /// Kafka Cosumer Hosted Service
    /// </summary>
    public class KafkaConsumerHostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> _logger;
        private ClusterClient _cluster;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            this._logger = logger;
            this._cluster = new ClusterClient(new Configuration
            {
                Seeds = "localhost:9092"
            }, new ConsoleLogger());
        }

        /// <summary>
        /// start Async
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task StartAsync(CancellationToken cancellationToken)
        {
            this._cluster.ConsumeFromLatest("test");
            this._cluster.MessageReceived += record =>
            {
                this._logger.LogInformation($"Received {Encoding.UTF8.GetString(record.Value as byte[])}");
            };
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            this._cluster?.Dispose();
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Kafka Producer Hosted Service
    /// </summary>
    public class KafkaProducerHostedService : IHostedService
    {
        public readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="logger"></param>
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
                
                Thread.Sleep(100);
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
