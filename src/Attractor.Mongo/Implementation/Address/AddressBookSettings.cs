using MongoDB.Driver;
using System;

namespace Attractor.Mongo.Implementation.Address
{
    internal sealed class AddressBookSettings
    {
        public MongoClientSettings ClientSettings { get; set; }

        public TimeSpan? AddressUsingTimeout { get; set; }

        public string DatabaseName { get; set; }

        public string CollectionName { get; set; }
    }
}
