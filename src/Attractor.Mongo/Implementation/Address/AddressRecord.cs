using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Attractor.Mongo.Implementation.Address
{
    internal sealed class AddressRecord
    {
        [BsonId]
        public byte[] Address { get; set; }

        public Guid UsingToken { get; set; }

        public DateTime ExpireAt { get; set; }
    }
}
