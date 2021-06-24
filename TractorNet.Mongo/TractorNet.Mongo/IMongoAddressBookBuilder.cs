using MongoDB.Bson.Serialization;
using System;

namespace TractorNet.Mongo
{
    public interface IMongoAddressBookBuilder
    {
        void UseDatabaseName(string name);

        void UseCollectionName(string name);

        void UseAddressUsingTimeout(TimeSpan time);

        void UseAddressSerializer(IBsonSerializer<byte[]> serializer);
    }
}
