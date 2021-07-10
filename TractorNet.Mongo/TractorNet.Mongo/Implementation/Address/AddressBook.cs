using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressBook : IAddressBook, IMongoAddressRegistration
    {
        private readonly IMongoCollection<AddressRecord> collection;
        private readonly IOptions<AddressBookSettings> options;

        public AddressBook(IOptions<AddressBookSettings> options)
        {
            collection = InitializeCollection(options.Value);

            this.options = options;
        }

        public async ValueTask<TryResult<IAsyncDisposable>> TryUseAddressAsync(IAddress address, CancellationToken token = default)
        {
            var expireTime = options.Value.AddressUsingTimeout.HasValue
                ? DateTime.UtcNow + options.Value.AddressUsingTimeout.Value
                : DateTime.MaxValue;
            var filter = Builders<AddressRecord>.Filter.And(
                Builders<AddressRecord>.Filter.Eq(record => record.Address, address.GetBytes().ToArray()),
                Builders<AddressRecord>.Filter.Gt(record => record.ExpireAt, DateTime.UtcNow));
            var update = Builders<AddressRecord>.Update.SetOnInsert(record => record.ExpireAt, expireTime);
            var updateOptions = new UpdateOptions { IsUpsert = true };

            var result = await collection.UpdateOneAsync(filter, update, updateOptions, token);

            return result.MatchedCount == 0
                ? new TrueResult<IAsyncDisposable>(new AddressRegistrationDisposable(this, result))
                : new FalseResult<IAsyncDisposable>();
        }

        public async ValueTask ProlongAddressUsingAsync(IAddress address, CancellationToken token = default)
        {
            if (!options.Value.AddressUsingTimeout.HasValue)
            {
                return;
            }

            var filter = Builders<AddressRecord>.Filter.And(
                Builders<AddressRecord>.Filter.Eq(record => record.Address, address.GetBytes().ToArray()),
                Builders<AddressRecord>.Filter.Gt(record => record.ExpireAt, DateTime.UtcNow));
            var update = Builders<AddressRecord>.Update.Set(record => record.ExpireAt, DateTime.UtcNow + options.Value.AddressUsingTimeout.Value);

            await collection.UpdateOneAsync(filter, update, null, token);
        }

        private IMongoCollection<AddressRecord> InitializeCollection(AddressBookSettings settings)
        {
            var indexBuilder = Builders<AddressRecord>.IndexKeys;
            var collection = new MongoClient(settings.ClientSettings)
                .GetDatabase(settings.DatabaseName)
                .GetCollection<AddressRecord>(settings.CollectionName);

            collection.Indexes.CreateOne(new CreateIndexModel<AddressRecord>(
                indexBuilder.Ascending(record => record.ExpireAt),
                new CreateIndexOptions
                {
                    Background = true,
                    ExpireAfter = TimeSpan.Zero
                }));

            collection.Indexes.CreateOne(new CreateIndexModel<AddressRecord>(
                indexBuilder.Ascending(record => record.Address),
                new CreateIndexOptions
                {
                    Background = true
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
