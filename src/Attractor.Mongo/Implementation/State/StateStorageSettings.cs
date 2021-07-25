using MongoDB.Driver;

namespace Attractor.Mongo.Implementation.State
{
    internal sealed class StateStorageSettings
    {
        public MongoClientSettings ClientSettings { get; set; }

        public string DatabaseName { get; set; }

        public string CollectionName { get; set; }
    }
}
