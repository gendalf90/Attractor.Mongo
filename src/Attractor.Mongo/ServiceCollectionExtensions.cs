using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using System;
using Attractor.Mongo.Implementation.Factory;

namespace Attractor.Mongo
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection UseMongoAddressBook(this IServiceCollection services, MongoClientSettings settings, Action<IMongoAddressBookBuilder> configuration = null)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            var builder = new MongoBuilder(services, settings);

            configuration?.Invoke(builder);

            builder.BuildAddressBook();

            return services;
        }

        public static IServiceCollection UseMongoMailbox(this IServiceCollection services, MongoClientSettings settings, Action<IMongoMailboxBuilder> configuration = null)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            var builder = new MongoBuilder(services, settings);

            configuration?.Invoke(builder);

            builder.BuildMailbox();

            return services;
        }

        public static IServiceCollection UseMongoState(this IServiceCollection services, MongoClientSettings settings, Action<IMongoStateBuilder> configuration = null)
        {
            if (services == null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            var builder = new MongoBuilder(services, settings);

            configuration?.Invoke(builder);

            builder.BuildState();

            return services;
        }
    }
}
