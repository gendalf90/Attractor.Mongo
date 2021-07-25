using Microsoft.Extensions.Options;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Attractor.Mongo.Implementation.Message
{
    internal sealed class Mailbox : IInbox, IAnonymousOutbox, IAsyncDisposable
    {
        private static readonly string DefaultDatabaseName = "attractor";
        private static readonly string DefaultCollectionName = "mailbox";
        private static readonly TimeSpan DefaultReadTrottleTime = TimeSpan.FromMilliseconds(1);
        private static readonly TimeSpan DefaultMessageProcessingTimeout = Timeout.InfiniteTimeSpan;
        private static readonly int DefaultMessageReadingBatchSize = 1;

        private readonly Channel<IProcessingMessage> readMessages = Channel.CreateUnbounded<IProcessingMessage>();

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
                token.ThrowIfCancellationRequested();

                if (readMessages.Reader.TryRead(out var message))
                {
                    yield return message;
                }
                else
                {
                    await RunWithTrottlingAsync(LoadBatchOfMessagesAsync);
                }
            }
        }

        private async ValueTask LoadBatchOfMessagesAsync()
        {
            var now = DateTime.UtcNow;
            var lockToken = Guid.NewGuid();
            var unlockTime = CalculateUnlockTime(now);
            var lockFilter = Builders<MessageRecord>.Filter.And(
                Builders<MessageRecord>.Filter.Lt(record => record.AvailableAt, now),
                Builders<MessageRecord>.Filter.Lt(record => record.UnlockedAt, now),
                Builders<MessageRecord>.Filter.Gt(record => record.ExpireAt, now));
            var lockUpdate = Builders<MessageRecord>.Update
                .Set(record => record.UnlockedAt, unlockTime)
                .Set(record => record.LockToken, lockToken);
            var readBatchSize = options.Value.MessagesReadingBatchSize ?? DefaultMessageReadingBatchSize;

            if (readBatchSize == DefaultMessageReadingBatchSize)
            {
                var record = await collection.FindOneAndUpdateAsync(lockFilter, lockUpdate, new FindOneAndUpdateOptions<MessageRecord>
                {
                    ReturnDocument = ReturnDocument.After
                });

                if (record != null)
                {
                    await TryAddMessageAsync(CreateProcessingMessage(record));
                }
            }
            else
            {
                var lockTokenFilter = Builders<MessageRecord>.Filter.Eq(record => record.LockToken, lockToken);
                var bulkUpdates = new List<WriteModel<MessageRecord>>(readBatchSize);

                for (int i = 0; i < readBatchSize; i++)
                {
                    bulkUpdates.Add(new UpdateOneModel<MessageRecord>(lockFilter, lockUpdate));
                }

                var result = await collection.BulkWriteAsync(bulkUpdates, new BulkWriteOptions
                {
                    IsOrdered = false
                });

                if (result.ModifiedCount == 0)
                {
                    return;
                }

                var records = await collection.Find(lockTokenFilter).ToListAsync();

                foreach (var record in records)
                {
                    await TryAddMessageAsync(CreateProcessingMessage(record));
                }
            }
        }

        private async ValueTask RunWithTrottlingAsync(Func<ValueTask> action)
        {
            var trottleTask = Task.Delay(options.Value.ReadTrottleTime ?? DefaultReadTrottleTime);

            try
            {
                await action();
            }
            finally
            {
                await trottleTask;
            }
        }

        private DateTime CalculateUnlockTime(DateTime now)
        {
            var timeout = options.Value.MessageProcessingTimeout ?? DefaultMessageProcessingTimeout;

            return timeout == Timeout.InfiniteTimeSpan ? DateTime.MaxValue : now + timeout;
        }

        private async ValueTask TryAddMessageAsync(IProcessingMessage message)
        {
            if (!readMessages.Writer.TryWrite(message))
            {
                await message.DisposeAsync();
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
            return SendAsync(
                to: address.GetBytes().ToArray(),
                payload: payload.GetBytes().ToArray(),
                metadata: metadata,
                token: token);
        }

        private async ValueTask SendAsync(byte[] to, byte[] payload, byte[] from = null, SendingMetadata metadata = null, CancellationToken token = default)
        {
            var record = new MessageRecord
            {
                To = to ?? throw new ArgumentNullException(nameof(to)),
                Payload = payload ?? throw new ArgumentNullException(nameof(payload)),
                From = from
            };

            if (metadata?.Delay != null)
            {
                record.AvailableAt = DateTime.UtcNow + metadata.Delay.Value;
            }

            if (metadata?.Ttl != null)
            {
                record.ExpireAt = DateTime.UtcNow + metadata.Ttl.Value;
            }
            else
            {
                record.ExpireAt = DateTime.MaxValue;
            }

            await collection.InsertOneAsync(record, null, token);
        }

        private IMongoCollection<MessageRecord> InitializeCollection(MailboxSettings settings)
        {
            var indexBuilder = Builders<MessageRecord>.IndexKeys;
            var collection = new MongoClient(settings.ClientSettings)
                .GetDatabase(settings.DatabaseName ?? DefaultDatabaseName)
                .GetCollection<MessageRecord>(settings.CollectionName ?? DefaultCollectionName);

            collection.Indexes.CreateOne(new CreateIndexModel<MessageRecord>(
                indexBuilder.Ascending(record => record.ExpireAt),
                new CreateIndexOptions
                {
                    Background = true,
                    ExpireAfter = TimeSpan.Zero
                }));

            collection.Indexes.CreateOne(new CreateIndexModel<MessageRecord>(
                indexBuilder.Ascending(record => record.AvailableAt),
                new CreateIndexOptions
                {
                    Background = true
                }));

            collection.Indexes.CreateOne(new CreateIndexModel<MessageRecord>(
                indexBuilder.Ascending(record => record.UnlockedAt),
                new CreateIndexOptions
                {
                    Background = true
                }));

            collection.Indexes.CreateOne(new CreateIndexModel<MessageRecord>(
                indexBuilder.Ascending(record => record.LockToken),
                new CreateIndexOptions
                {
                    Background = true
                }));

            return collection;
        }

        public async ValueTask DisposeAsync()
        {
            readMessages.Writer.TryComplete();

            while (readMessages.Reader.TryRead(out var message))
            {
                await message.DisposeAsync();
            }
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
                var filter = Builders<MessageRecord>.Filter.And(
                    Builders<MessageRecord>.Filter.Eq(record => record.Id, messageRecord.Id),
                    Builders<MessageRecord>.Filter.Eq(record => record.LockToken, messageRecord.LockToken));

                await mailbox.collection.DeleteOneAsync(filter, token);
            }

            public async ValueTask DelayAsync(TimeSpan time, CancellationToken token = default)
            {
                var filter = Builders<MessageRecord>.Filter.And(
                    Builders<MessageRecord>.Filter.Eq(record => record.Id, messageRecord.Id),
                    Builders<MessageRecord>.Filter.Eq(record => record.LockToken, messageRecord.LockToken));
                var update = Builders<MessageRecord>.Update.Set(record => record.AvailableAt, DateTime.UtcNow + time);

                await mailbox.collection.UpdateOneAsync(filter, update, null, token);
            }

            public async ValueTask ExpireAsync(TimeSpan time, CancellationToken token = default)
            {
                var filter = Builders<MessageRecord>.Filter.And(
                    Builders<MessageRecord>.Filter.Eq(record => record.Id, messageRecord.Id),
                    Builders<MessageRecord>.Filter.Eq(record => record.LockToken, messageRecord.LockToken),
                    Builders<MessageRecord>.Filter.Gt(record => record.ExpireAt, DateTime.UtcNow));
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
                return mailbox.SendAsync(
                    to: address.GetBytes().ToArray(),
                    payload: payload.GetBytes().ToArray(),
                    from: messageRecord.To,
                    metadata: metadata,
                    token: token);
            }

            public ValueTask SendMessageAsync(IPayload payload, SendingMetadata metadata = null, CancellationToken token = default)
            {
                return mailbox.SendAsync(
                    to: messageRecord.To,
                    payload: payload.GetBytes().ToArray(),
                    from: messageRecord.To,
                    metadata: metadata,
                    token: token);
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
                return mailbox.SendAsync(
                    to: messageRecord.From,
                    payload: payload.GetBytes().ToArray(),
                    from: messageRecord.To,
                    metadata: metadata,
                    token: token);
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
                var filter = Builders<MessageRecord>.Filter.And(
                    Builders<MessageRecord>.Filter.Eq(record => record.Id, record.Id),
                    Builders<MessageRecord>.Filter.Eq(record => record.LockToken, record.LockToken));
                var update = Builders<MessageRecord>.Update
                    .Set(record => record.UnlockedAt, DateTime.MinValue)
                    .Set(record => record.LockToken, Guid.Empty);

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
