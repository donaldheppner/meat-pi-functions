using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos.Table;
using System.Threading.Tasks;

namespace MeatPi.Functions
{
    public class ReadingValue
    {
        [JsonPropertyName("pin")]
        public int Pin { get; set; }

        [JsonPropertyName("value")]
        public int Value { get; set; }

        [JsonPropertyName("resistance")]
        public double Resistance { get; set; }

        [JsonPropertyName("kelvins")]
        public double Kelvins { get; set; }
    }

    public class CookReadingValue
    {
        [JsonPropertyName("device_id")]
        public string DeviceId { get; set; }

        [JsonPropertyName("cook_id")]
        public string CookId { get; set; }

        [JsonPropertyName("time")]
        public string Time { get; set; }

        [JsonPropertyName("chamber_target")]
        public double ChamberTarget { get; set; }

        [JsonPropertyName("cooker_on")]
        public bool IsCookerOn { get; set; }

        [JsonPropertyName("readings")]
        public List<ReadingValue> Readings { get; set; }
    }

    public class ReadingTable : TableEntity
    {
        public const string TableName = "Reading";

        public ReadingTable() { }
        public ReadingTable(string deviceId, string cookId) : base(deviceId, cookId) { }

        public string DeviceId => PartitionKey;
        public string CookId => RowKey;

        public string ReadingTime { get; set; }
        public double ChamberTarget { get; set; }
        public bool IsCookerOn { get; set; }
        public string Readings { get; set; }

        public static ReadingTable FromReading(CookReadingValue reading)
        {
            return new ReadingTable(reading.DeviceId, reading.CookId)
            {
                ReadingTime = reading.Time,
                ChamberTarget = reading.ChamberTarget,
                IsCookerOn = reading.IsCookerOn,
                Readings = JsonSerializer.Serialize<List<ReadingValue>>(reading.Readings)
            };
        }
    }


    public static class TemperatureReadingFunctions
    {
        private static readonly CloudStorageAccount StorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        private static readonly SortedSet<string> CreatedTables = new SortedSet<string>();
        private const string ReadingsQueue = "readings";

        /// <summary>
        /// Is triggered when an event comes in via service bus; stores the data in a table and writes it to a queue for the web app to process
        /// </summary>
        [FunctionName("QueueReading")]
        public static async Task Run([ServiceBusTrigger("readings", Connection = "meatpi_SERVICEBUS")] string readingsQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {readingsQueueItem}");

            var reading = JsonSerializer.Deserialize<CookReadingValue>(readingsQueueItem);
            var table = ReadingTable.FromReading(reading);
            await AzureTableHelper.InsertOrReplace<ReadingTable>(ReadingTable.TableName, table);
            await AzureQueueStorageHelper.QueueMessage(ReadingsQueue, reading);
        }
    }
}
