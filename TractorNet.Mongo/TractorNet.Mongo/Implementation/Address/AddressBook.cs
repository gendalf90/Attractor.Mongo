using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressBook : IAddressBook
    {
        private static readonly string DefaultDatabaseName = "tractor";
        private static readonly string DefaultCollectionName = "addressBook";
        private static readonly TimeSpan DefaultAddressUsingTimeout = Timeout.InfiniteTimeSpan;

        private readonly IMongoCollection<AddressRecord> collection;
        private readonly IOptions<AddressBookSettings> options;

        public AddressBook(IOptions<AddressBookSettings> options)
        {
            collection = InitializeCollection(options.Value);

            this.options = options;
        }

        public async ValueTask<TryResult<IAsyncDisposable>> TryUseAddressAsync(IAddress address, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            var now = DateTime.UtcNow;
            var expirationTime = CalculateExpirationTime(now);
            var filter = Builders<AddressRecord>.Filter.And(
                Builders<AddressRecord>.Filter.Eq(record => record.Address, address.GetBytes().ToArray()),
                Builders<AddressRecord>.Filter.Gt(record => record.ExpireAt, now));
            var update = Builders<AddressRecord>.Update.SetOnInsert(record => record.ExpireAt, expirationTime);

            var result = await collection.UpdateOneAsync(filter, update, new UpdateOptions
            {
                IsUpsert = true
            });

            return result.MatchedCount == 0
                ? new TrueResult<IAsyncDisposable>(new AddressRegistrationDisposable(this, result))
                : new FalseResult<IAsyncDisposable>();
        }

        private DateTime CalculateExpirationTime(DateTime now)
        {
            var timeout = options.Value.AddressUsingTimeout ?? DefaultAddressUsingTimeout;

            return timeout == Timeout.InfiniteTimeSpan ? DateTime.MaxValue : now + timeout;
        }

        private IMongoCollection<AddressRecord> InitializeCollection(AddressBookSettings settings)
        {
            var indexBuilder = Builders<AddressRecord>.IndexKeys;
            var collection = new MongoClient(settings.ClientSettings)
                .GetDatabase(settings.DatabaseName ?? DefaultDatabaseName)
                .GetCollection<AddressRecord>(settings.CollectionName ?? DefaultCollectionName);

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
