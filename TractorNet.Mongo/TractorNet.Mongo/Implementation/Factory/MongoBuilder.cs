using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using System;
using TractorNet.Mongo.Implementation.Address;
using TractorNet.Mongo.Implementation.Message;

namespace TractorNet.Mongo.Implementation.Factory
{
    internal sealed class MongoBuilder : IMongoAddressBookBuilder, IMongoMailboxBuilder
    {
        private readonly IServiceCollection services;
        private readonly MongoClientSettings mongoClientSettings;

        public MongoBuilder(IServiceCollection services, MongoClientSettings mongoClientSettings)
        {
            this.services = services;
            this.mongoClientSettings = mongoClientSettings;
        }

        void IMongoAddressBookBuilder.UseAddressUsingTimeout(TimeSpan time)
        {
            services.Configure<AddressBookSettings>(settings =>
            {
                settings.AddressUsingTimeout = time;
            });
        }

        void IMongoAddressBookBuilder.UseCollectionName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException(nameof(name));
            }

            services.Configure<AddressBookSettings>(settings =>
            {
                settings.CollectionName = name;
            });
        }

        void IMongoAddressBookBuilder.UseDatabaseName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException(nameof(name));
            }

            services.Configure<AddressBookSettings>(settings =>
            {
                settings.DatabaseName = name;
            });
        }

        void IMongoMailboxBuilder.UseDatabaseName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException(nameof(name));
            }

            services.Configure<MailboxSettings>(settings =>
            {
                settings.DatabaseName = name;
            });
        }

        void IMongoMailboxBuilder.UseCollectionName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException(nameof(name));
            }

            services.Configure<MailboxSettings>(settings =>
            {
                settings.CollectionName = name;
            });
        }

        void IMongoMailboxBuilder.UseMessageProcessingTimeout(TimeSpan time)
        {
            services.Configure<MailboxSettings>(settings =>
            {
                settings.MessageProcessingTimeout = time;
            });
        }

        void IMongoMailboxBuilder.UseReadTrottleTime(TimeSpan time)
        {
            services.Configure<MailboxSettings>(settings =>
            {
                settings.ReadTrottleTime = time;
            });
        }

        void IMongoMailboxBuilder.UseReadBatchSize(int size)
        {
            if (size <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }

            services.Configure<MailboxSettings>(settings =>
            {
                settings.MessagesReadingBatchSize = size;
            });
        }

        public void BuildAddressBook()
        {
            services.Configure<AddressBookSettings>(settings =>
            {
                settings.ClientSettings = mongoClientSettings;
            });

            services.AddSingleton<IAddressBook, AddressBook>();
        }

        public void BuildMailbox()
        {
            services.Configure<MailboxSettings>(settings =>
            {
                settings.ClientSettings = mongoClientSettings;
            });

            services.AddSingleton<Mailbox>();
            services.AddSingleton<IInbox>(provider => provider.GetRequiredService<Mailbox>());
            services.AddSingleton<IAnonymousOutbox>(provider => provider.GetRequiredService<Mailbox>());
        }
    }
}
