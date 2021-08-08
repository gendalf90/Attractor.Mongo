using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Attractor.Mongo.Implementation.State
{
    internal sealed class StateStorage : IStateStorage
    {
        private static readonly string DefaultDatabaseName = "tractor";
        private static readonly string DefaultCollectionName = "stateStorage";

        private readonly IMongoCollection<StateRecord> collection;

        public StateStorage(IOptions<StateStorageSettings> options)
        {
            collection = InitializeCollection(options.Value);
        }

        public async ValueTask LoadStateAsync(IProcessingMessage message, CancellationToken token = default)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            var addressBytes = message.GetBytes().ToArray();
            var filter = Builders<StateRecord>.Filter.Eq(record => record.Address, addressBytes);

            var stateRecord = await collection.Find(filter).FirstOrDefaultAsync(token);

            stateRecord ??= new StateRecord
            {
                Address = addressBytes,
                Data = Array.Empty<byte>()
            };

            message.SetFeature<IStateFeature>(new StateFeature(stateRecord, this));
        }

        private IMongoCollection<StateRecord> InitializeCollection(StateStorageSettings settings)
        {
            return new MongoClient(settings.ClientSettings)
                .GetDatabase(settings.DatabaseName ?? DefaultDatabaseName)
                .GetCollection<StateRecord>(settings.CollectionName ?? DefaultCollectionName);
        }

        private class StateFeature : IStateFeature
        {
            private readonly StateRecord currentRecord;
            private readonly StateStorage storage;

            public StateFeature(StateRecord currentRecord, StateStorage storage)
            {
                this.currentRecord = currentRecord;
                this.storage = storage;
            }

            public ReadOnlyMemory<byte> GetBytes()
            {
                return currentRecord.Data;
            }

            public async ValueTask SaveAsync(IState state, CancellationToken token = default)
            {
                if (state == null)
                {
                    throw new ArgumentNullException(nameof(state));
                }

                var stateBytes = state.GetBytes().ToArray();
                var filter = Builders<StateRecord>.Filter.Eq(record => record.Address, currentRecord.Address);
                var update = Builders<StateRecord>.Update.Set(record => record.Data, stateBytes);
                var options = new UpdateOptions { IsUpsert = true };

                await storage.collection.UpdateOneAsync(filter, update, options, token);

                currentRecord.Data = stateBytes;
            }

            public async ValueTask ClearAsync(CancellationToken token = default)
            {
                var filter = Builders<StateRecord>.Filter.Eq(record => record.Address, currentRecord.Address);

                await storage.collection.DeleteOneAsync(filter, token);

                currentRecord.Data = Array.Empty<byte>();
            }
        }
    }
}
