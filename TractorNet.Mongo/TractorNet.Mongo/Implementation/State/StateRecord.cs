using MongoDB.Bson.Serialization.Attributes;

namespace TractorNet.Mongo.Implementation.State
{
    internal sealed class StateRecord
    {
        [BsonId]
        public byte[] Address { get; set; }

        public byte[] Data { get; set; }
    }
}
