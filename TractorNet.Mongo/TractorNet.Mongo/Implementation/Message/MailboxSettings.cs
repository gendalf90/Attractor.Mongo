using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;

namespace TractorNet.Mongo.Implementation.Message
{
    internal sealed class MailboxSettings
    {
        public MongoClientSettings ClientSettings { get; set; }

        public string DatabaseName { get; set; } = "tractor";

        public string CollectionName { get; set; } = "mailbox";

        public IBsonSerializer<byte[]> AddressSerializer { get; set; }

        public IBsonSerializer<byte[]> PayloadSerializer { get; set; }

        public TimeSpan? ReadTrottleTime { get; set; } = TimeSpan.FromMilliseconds(1);

        public TimeSpan? MessageProcessingTimeout { get; set; }
    }
}
