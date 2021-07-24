using MongoDB.Bson.Serialization.Attributes;
using System;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressRecord
    {
        [BsonId]
        public byte[] Address { get; set; }

        public Guid UsingToken { get; set; }

        public DateTime ExpireAt { get; set; }
    }
}
