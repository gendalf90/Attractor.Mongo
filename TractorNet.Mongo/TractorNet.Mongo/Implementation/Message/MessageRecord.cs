using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace TractorNet.Mongo.Implementation.Message
{
    internal sealed class MessageRecord
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonIgnoreIfNull]
        public byte[] From { get; set; }

        public byte[] To { get; set; }

        public byte[] Payload { get; set; }

        public DateTime ExpireAt { get; set; }

        public DateTime AvailableAt { get; set; }

        public DateTime UnlockedAt { get; set; }

        public Guid LockToken { get; set; }
    }
}
