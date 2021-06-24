using MongoDB.Bson.Serialization;
using System;

namespace TractorNet.Mongo
{
    public interface IMongoMailboxBuilder
    {
        void UseDatabaseName(string name);

        void UseCollectionName(string name);

        void UseAddressSerializer(IBsonSerializer<byte[]> serializer);

        void UsePayloadSerializer(IBsonSerializer<byte[]> serializer);

        void UseMessageProcessingTimeout(TimeSpan time);

        void UseReadTrottleTime(TimeSpan time);
    }
}
