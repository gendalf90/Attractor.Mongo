using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressRecord
    {
        [BsonId]
        public ObjectId Id { get; set; }

        public byte[] Address { get; set; }

        public DateTime ExpireAt { get; set; }
    }
}
