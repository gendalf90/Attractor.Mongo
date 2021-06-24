using Microsoft.Extensions.Options;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressBook : IAddressBook, IMongoAddressRegistration
    {
        private readonly IMongoCollection<AddressRecord> collection;

        public AddressBook(IOptions<AddressBookSettings> options)
        {
            collection = InitializeCollection(options.Value);
        }

        public async ValueTask<TryResult<IAsyncDisposable>> TryUseAddressAsync(IAddress address, CancellationToken token = default)
        {
            var filter = Builders<AddressRecord>.Filter.Eq(record => record.Address, address.GetBytes().ToArray());
            var update = Builders<AddressRecord>.Update.SetOnInsert(record => record.RegisteredAt, DateTime.UtcNow);
            var options = new UpdateOptions { IsUpsert = true };

            var result = await collection.UpdateOneAsync(filter, update, options, token);

            return result.ModifiedCount == 0
                ? new TrueResult<IAsyncDisposable>(new AddressRegistrationDisposable(this, result))
                : new FalseResult<IAsyncDisposable>();
        }

        public async ValueTask ProlongAddressUsingAsync(IAddress address, CancellationToken token = default)
        {
            var filter = Builders<AddressRecord>.Filter.Eq(record => record.Address, address.GetBytes().ToArray());
            var update = Builders<AddressRecord>.Update.Set(record => record.RegisteredAt, DateTime.UtcNow);

            await collection.UpdateOneAsync(filter, update, null, token);
        }

        private IMongoCollection<AddressRecord> InitializeCollection(AddressBookSettings settings)
        {
            BsonClassMap.RegisterClassMap<AddressRecord>(map =>
            {
                map.MapIdProperty(record => record.Id).SetIdGenerator(ObjectIdGenerator.Instance);

                var addressMap = map.MapProperty(record => record.Address);

                if (settings.AddressSerializer != null)
                {
                    addressMap.SetSerializer(settings.AddressSerializer);
                }

                map.MapProperty(record => record.RegisteredAt);
            });

            var indexBuilder = Builders<AddressRecord>.IndexKeys;
            var collection = new MongoClient(settings.ClientSettings)
                .GetDatabase(settings.DatabaseName)
                .GetCollection<AddressRecord>(settings.CollectionName);

            if (settings.AddressUsingTimeout.HasValue)
            {
                collection.Indexes.CreateOne(new CreateIndexModel<AddressRecord>(
                    indexBuilder.Ascending(record => record.RegisteredAt),
                    new CreateIndexOptions
                    {
                        Background = true,
                        ExpireAfter = settings.AddressUsingTimeout.Value
                    }));
            }

            collection.Indexes.CreateOne(new CreateIndexModel<AddressRecord>(
                    indexBuilder.Ascending(record => record.Address),
                    new CreateIndexOptions
                    {
                        Background = true,
                        Unique = true
                    }));

            return collection;
        }

        private class AddressRegistrationDisposable : IAsyncDisposable
        {
            private readonly AddressBook addressBook;
            private readonly UpdateResult updateResult;

            public AddressRegistrationDisposable(AddressBook addressBook, UpdateResult updateResult)
            {
                this.addressBook = addressBook;
                this.updateResult = updateResult;
            }

            public async ValueTask DisposeAsync()
            {
                var filter = Builders<AddressRecord>.Filter.Eq(record => record.Id, updateResult.UpsertedId.AsObjectId);

                await addressBook.collection.DeleteOneAsync(filter);
            }
        }
    }
}
