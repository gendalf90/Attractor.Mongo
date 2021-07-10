using MongoDB.Driver;
using System;

namespace TractorNet.Mongo.Implementation.Message
{
    internal sealed class MailboxSettings
    {
        public MongoClientSettings ClientSettings { get; set; }

        public string DatabaseName { get; set; } = "tractor";

        public string CollectionName { get; set; } = "mailbox";

        public TimeSpan? ReadTrottleTime { get; set; } = TimeSpan.FromMilliseconds(1);

        public TimeSpan? MessageProcessingTimeout { get; set; }
    }
}
