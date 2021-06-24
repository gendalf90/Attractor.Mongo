using MongoDB.Bson;
using System;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressRecord
    {
        public ObjectId Id { get; set; }

        public byte[] Address { get; set; }

        public DateTime RegisteredAt { get; set; }
    }
}
