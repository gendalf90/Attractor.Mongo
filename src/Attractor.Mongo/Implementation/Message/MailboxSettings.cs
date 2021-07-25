using MongoDB.Driver;
using System;

namespace Attractor.Mongo.Implementation.Message
{
    internal sealed class MailboxSettings
    {
        public MongoClientSettings ClientSettings { get; set; }

        public string DatabaseName { get; set; }

        public string CollectionName { get; set; }

        public TimeSpan? ReadTrottleTime { get; set; }

        public TimeSpan? MessageProcessingTimeout { get; set; }

        public int? MessagesReadingBatchSize { get; set; }
    }
}
