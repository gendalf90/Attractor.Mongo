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

        public async ValueTask<TryResult<IAsyncDisposable>> TryUseAddressAsync(IProcessingMessage message, CancellationToken token = default)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            token.ThrowIfCancellationRequested();

            var now = DateTime.UtcNow;
            var usingToken = Guid.NewGuid();
            var addressBytes = message.GetBytes().ToArray();
            var expirationTime = CalculateExpirationTime(now);
            var expirationFilter = Builders<AddressRecord>.Filter.And(
                Builders<AddressRecord>.Filter.Eq(record => record.Address, addressBytes),
                Builders<AddressRecord>.Filter.Lt(record => record.ExpireAt, now));
            var expirationUpdate = Builders<AddressRecord>.Update
                .Set(record => record.ExpireAt, expirationTime)
                .Set(record => record.UsingToken, usingToken);
            var insertingFilter = Builders<AddressRecord>.Filter.Eq(record => record.Address, addressBytes);
            var insertingUpdate = Builders<AddressRecord>.Update
                .SetOnInsert(record => record.ExpireAt, expirationTime)
                .SetOnInsert(record => record.UsingToken, usingToken);
            var bulkUpdates = new WriteModel<AddressRecord>[]
            {
                new UpdateOneModel<AddressRecord>(expirationFilter, expirationUpdate),
                new UpdateOneModel<AddressRecord>(insertingFilter, insertingUpdate) { IsUpsert = true }
            };

            var result = await collection.BulkWriteAsync(bulkUpdates, new BulkWriteOptions
            {
                IsOrdered = false
            });

            if (result.ModifiedCount == 0 && result.MatchedCount > 0)
            {
                return new FalseResult<IAsyncDisposable>();
            }

            var registration = new AddressRegistration(this, usingToken);

            message.SetFeature<IMongoAddressFeature>(registration);

            return new TrueResult<IAsyncDisposable>(registration);
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
                indexBuilder.Ascending(record => record.UsingToken),
                new CreateIndexOptions
                {
                    Background = true
                }));

            return collection;
        }

        private class AddressRegistration : IMongoAddressFeature, IAsyncDisposable
        {
            private readonly AddressBook addressBook;
            private readonly Guid usingToken;

            public AddressRegistration(AddressBook addressBook, Guid usingToken)
            {
                this.addressBook = addressBook;
                this.usingToken = usingToken;
            }

            public async ValueTask DisposeAsync()
            {
                var filter = Builders<AddressRecord>.Filter.Eq(record => record.UsingToken, usingToken);

                await addressBook.collection.DeleteOneAsync(filter);
            }

            public async ValueTask ProlongAddressUsingAsync(CancellationToken token = default)
            {
                var expirationTime = addressBook.CalculateExpirationTime(DateTime.UtcNow);
                var filter = Builders<AddressRecord>.Filter.Eq(record => record.UsingToken, usingToken);
                var update = Builders<AddressRecord>.Update.Set(record => record.ExpireAt, expirationTime);

                await addressBook.collection.UpdateOneAsync(filter, update, null, token);
            }
        }
    }
}
