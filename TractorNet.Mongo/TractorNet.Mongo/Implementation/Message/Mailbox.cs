using Microsoft.Extensions.Options;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo.Implementation.Message
{
    internal sealed class Mailbox : IInbox, IAnonymousOutbox
    {
        private readonly IMongoCollection<MessageRecord> collection;
        private readonly IOptions<MailboxSettings> options;
        
        public Mailbox(IOptions<MailboxSettings> options)
        {
            collection = InitializeCollection(options.Value);

            this.options = options;
        }

        public async IAsyncEnumerable<IProcessingMessage> ReadMessagesAsync([EnumeratorCancellation] CancellationToken token = default)
        {
            while (true)
            {
                var unlockTime = options.Value.MessageProcessingTimeout.HasValue
                    ? DateTime.UtcNow + options.Value.MessageProcessingTimeout.Value
                    : DateTime.MaxValue;
                var filter = Builders<MessageRecord>.Filter.And(
                    Builders<MessageRecord>.Filter.Lte(record => record.AvailableAt, DateTime.UtcNow),
                    Builders<MessageRecord>.Filter.Lte(record => record.UnlockedAt, DateTime.UtcNow));
                var update = Builders<MessageRecord>.Update.Set(record => record.UnlockedAt, unlockTime);
                var findOptions = new FindOneAndUpdateOptions<MessageRecord, MessageRecord> { ReturnDocument = ReturnDocument.After };
                var trottleTask = options.Value.ReadTrottleTime.HasValue
                    ? Task.Delay(options.Value.ReadTrottleTime.Value, token)
                    : Task.CompletedTask;
                var findTask = collection.FindOneAndUpdateAsync(filter, update, findOptions, token);

                await Task.WhenAll(trottleTask, findTask);

                if (findTask.Result != null)
                {
                    yield return CreateProcessingMessage(findTask.Result);
                }
            }
        }

        private ProcessingMessage CreateProcessingMessage(MessageRecord record)
        {
            var result = new ProcessingMessage(this, record);

            result.SetFeature<IReceivedMessageFeature>(new ReceivedMessageFeature(this, record));
            result.SetFeature<ISelfFeature>(new SelfFeature(this, record));

            if (record.From != null)
            {
                result.SetFeature<ISenderFeature>(new SenderFeature(this, record));
            }

            return result;
        }

        public ValueTask SendMessageAsync(IAddress address, IPayload payload, SendingMetadata metadata = null, CancellationToken token = default)
        {
            var record = new MessageRecord
            {
                To = address.GetBytes().ToArray(),
                Payload = payload.GetBytes().ToArray()
            };

            return SendAsync(record, metadata, token);
        }

        private async ValueTask SendAsync(MessageRecord record, SendingMetadata metadata, CancellationToken token)
        {
            if (record.To == null)
            {
                throw new ArgumentNullException(nameof(record.To));
            }

            if (record.Payload == null)
            {
                throw new ArgumentNullException(nameof(record.Payload));
            }

            if (metadata?.Delay != null)
            {
                record.AvailableAt = DateTime.UtcNow + metadata.Delay.Value;
            }

            if (metadata?.Ttl != null)
            {
                record.ExpireAt = DateTime.UtcNow + metadata.Ttl.Value;
            }

            await collection.InsertOneAsync(record, null, token);
        }

        private IMongoCollection<MessageRecord> InitializeCollection(MailboxSettings settings)
        {
            BsonClassMap.RegisterClassMap<MessageRecord>(map =>
            {
                map.MapIdProperty(record => record.Id).SetIdGenerator(ObjectIdGenerator.Instance);

                var addressMaps = new List<BsonMemberMap>
                {
                    map.MapProperty(record => record.To),
                    map.MapProperty(record => record.From).SetIgnoreIfNull(true)
                };

                if (settings.AddressSerializer != null)
                {
                    addressMaps.ForEach(addressMap => addressMap.SetSerializer(settings.AddressSerializer));
                }

                var payloadMap = map.MapProperty(record => record.Payload);

                if (settings.PayloadSerializer != null)
                {
                    payloadMap.SetSerializer(settings.PayloadSerializer);
                }

                map.MapProperty(record => record.ExpireAt);
                map.MapProperty(record => record.AvailableAt);
                map.MapProperty(record => record.UnlockedAt);
            });

            var indexBuilder = Builders<MessageRecord>.IndexKeys;
            var collection = new MongoClient(settings.ClientSettings)
                .GetDatabase(settings.DatabaseName)
                .GetCollection<MessageRecord>(settings.CollectionName);

            collection.Indexes.CreateOne(new CreateIndexModel<MessageRecord>(
                indexBuilder.Ascending(record => record.ExpireAt),
                new CreateIndexOptions
                {
                    Background = true,
                    ExpireAfter = TimeSpan.Zero
                }));

            collection.Indexes.CreateOne(new CreateIndexModel<MessageRecord>(
                indexBuilder.Combine(
                    indexBuilder.Ascending(record => record.AvailableAt),
                    indexBuilder.Ascending(record => record.UnlockedAt)),
                new CreateIndexOptions
                {
                    Background = true
                }));

            return collection;
        }

        private class ReceivedMessageFeature : IReceivedMessageFeature
        {
            private readonly MessageRecord messageRecord;
            private readonly Mailbox mailbox;

            public ReceivedMessageFeature(Mailbox mailbox, MessageRecord messageRecord)
            {
                this.mailbox = mailbox;
                this.messageRecord = messageRecord;
            }

            public async ValueTask ConsumeAsync(CancellationToken token = default)
            {
                var filter = Builders<MessageRecord>.Filter.Eq(record => record.Id, messageRecord.Id);

                await mailbox.collection.DeleteOneAsync(filter, token);
            }

            public async ValueTask DelayAsync(TimeSpan time, CancellationToken token = default)
            {
                var filter = Builders<MessageRecord>.Filter.Eq(record => record.Id, messageRecord.Id);
                var update = Builders<MessageRecord>.Update.Set(record => record.AvailableAt, DateTime.UtcNow + time);

                await mailbox.collection.UpdateOneAsync(filter, update, null, token);
            }

            public async ValueTask ExpireAsync(TimeSpan time, CancellationToken token = default)
            {
                var filter = Builders<MessageRecord>.Filter.Eq(record => record.Id, messageRecord.Id);
                var update = Builders<MessageRecord>.Update.Set(record => record.ExpireAt, DateTime.UtcNow + time);

                await mailbox.collection.UpdateOneAsync(filter, update, null, token);
            }

            ReadOnlyMemory<byte> IAddress.GetBytes()
            {
                return messageRecord.To;
            }

            ReadOnlyMemory<byte> IPayload.GetBytes()
            {
                return messageRecord.Payload;
            }
        }

        private class SelfFeature : ISelfFeature
        {
            private readonly MessageRecord messageRecord;
            private readonly Mailbox mailbox;

            public SelfFeature(Mailbox mailbox, MessageRecord messageRecord)
            {
                this.mailbox = mailbox;
                this.messageRecord = messageRecord;
            }

            public ReadOnlyMemory<byte> GetBytes()
            {
                return messageRecord.To;
            }

            public ValueTask SendMessageAsync(IAddress address, IPayload payload, SendingMetadata metadata = null, CancellationToken token = default)
            {
                var record = new MessageRecord
                {
                    To = address.GetBytes().ToArray(),
                    Payload = payload.GetBytes().ToArray(),
                    From = messageRecord.To
                };

                return mailbox.SendAsync(record, metadata, token);
            }

            public ValueTask SendMessageAsync(IPayload payload, SendingMetadata metadata = null, CancellationToken token = default)
            {
                var record = new MessageRecord
                {
                    To = messageRecord.To,
                    Payload = payload.GetBytes().ToArray(),
                    From = messageRecord.To
                };

                return mailbox.SendAsync(record, metadata, token);
            }
        }

        private class SenderFeature : ISenderFeature
        {
            private readonly MessageRecord messageRecord;
            private readonly Mailbox mailbox;

            public SenderFeature(Mailbox mailbox, MessageRecord messageRecord)
            {
                this.mailbox = mailbox;
                this.messageRecord = messageRecord;
            }

            public ReadOnlyMemory<byte> GetBytes()
            {
                return messageRecord.From;
            }

            public ValueTask SendMessageAsync(IPayload payload, SendingMetadata metadata = null, CancellationToken token = default)
            {
                var record = new MessageRecord
                {
                    To = messageRecord.From,
                    Payload = payload.GetBytes().ToArray(),
                    From = messageRecord.To
                };

                return mailbox.SendAsync(record, metadata, token);
            }
        }

        private class ProcessingMessage : IProcessingMessage
        {
            private readonly ConcurrentDictionary<Type, object> features = new ConcurrentDictionary<Type, object>();

            private readonly MessageRecord record;
            private readonly Mailbox mailbox;

            public ProcessingMessage(Mailbox mailbox, MessageRecord record)
            {
                this.mailbox = mailbox;
                this.record = record;
            }

            public async ValueTask DisposeAsync()
            {
                var filter = Builders<MessageRecord>.Filter.Eq(record => record.Id, record.Id);
                var update = Builders<MessageRecord>.Update.Set(record => record.UnlockedAt, DateTime.MinValue);

                await mailbox.collection.UpdateOneAsync(filter, update);
            }

            public ReadOnlyMemory<byte> GetBytes()
            {
                return record.To;
            }

            public T GetFeature<T>() where T : class
            {
                if (features.TryGetValue(typeof(T), out var result))
                {
                    return (T)result;
                }

                return null;
            }

            public void SetFeature<T>(T feature) where T : class
            {
                features.AddOrUpdate(typeof(T), feature, (_, _) => feature);
            }
        }
    }
}
