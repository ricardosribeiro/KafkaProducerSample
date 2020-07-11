using Confluent.Kafka;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaProducerSample.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = "omnibus-01.srvs.cloudkafka.com:9094,omnibus-02.srvs.cloudkafka.com:9094,omnibus-03.srvs.cloudkafka.com:9094",
                SaslUsername = "p5yt75io",
                SaslPassword= "UtSNvphLpUW01YR_tPTWgC_SQT2G7xUd",
                SaslMechanism = SaslMechanism.ScramSha256,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                EnableSslCertificateVerification=false
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                int count=0;
                while (true)
                {
                    var t = producer.ProduceAsync("p5yt75io-test", new Message<Null, string> { Value = $"message {count++}" });                  
                    t.ContinueWith(t =>
                    {
                        if (!t.IsFaulted)
                        {
                            System.Console.WriteLine($"Delivered: {t.Result} to {t.Result.TopicPartitionOffset}");
                        }
                        else
                        {

                            System.Console.WriteLine($"Delivery failed: {t.Result.Offset}");
                        }
                    });

                }
            }
        }
    }
}
