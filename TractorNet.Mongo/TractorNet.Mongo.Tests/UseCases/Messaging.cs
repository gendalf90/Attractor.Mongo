using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;

namespace TractorNet.Mongo.Tests.UseCases
{
    public class Messaging
    {
        private const int MaxRunningNumberForTheSameAddress = 1;

        [Fact]
        public async Task RunWithRunningNumberChecking()
        {
            // Arrange
            var testAddress = TestBytesBuffer.Generate();
            var currentRunningNumber = 0;
            var resultChannel = Channel.CreateUnbounded<int>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        await resultChannel.Writer.WriteAsync(Interlocked.Increment(ref currentRunningNumber));

                        await Task.Delay(TimeSpan.FromMilliseconds(100));

                        await context
                            .Metadata
                            .GetFeature<IReceivedMessageFeature>()
                            .ConsumeAsync();

                        Interlocked.Decrement(ref currentRunningNumber);
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy(_ => testAddress);
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            for (int i = 0; i < 10; i++)
            {
                await outbox.SendMessageAsync(testAddress, TestBytesBuffer.Create());
            }

            // Assert
            for (int i = 0; i < 10; i++)
            {
                Assert.Equal(MaxRunningNumberForTheSameAddress, await resultChannel.Reader.ReadAsync());
            }

            await host.StopAsync();
        }

        [Fact]
        public async Task RunWithDelayingMessage()
        {
            // Arrange
            var testAddress = TestBytesBuffer.Generate();
            var resultsChannel = Channel.CreateUnbounded<DateTime>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        await resultsChannel.Writer.WriteAsync(DateTime.UtcNow);
                        await context
                            .Metadata
                            .GetFeature<IReceivedMessageFeature>()
                            .ConsumeAsync();
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy(_ => testAddress);
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            var timeOfSending = DateTime.UtcNow;

            await outbox.SendMessageAsync(testAddress, TestBytesBuffer.Create(), new SendingMetadata
            {
                Delay = TimeSpan.FromSeconds(1)
            });

            var timeOfReceiving = await resultsChannel.Reader.ReadAsync();

            await host.StopAsync();

            // Assert
            Assert.True(timeOfReceiving - timeOfSending >= TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task RunWithExpiringMessage()
        {
            // Arrange
            var testAddress = TestBytesBuffer.Generate();
            var resultsChannel = Channel.CreateUnbounded<IMessage>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var feature = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultsChannel.Writer.WriteAsync(feature);
                        await feature.ConsumeAsync();
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy(_ => testAddress);
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            await outbox.SendMessageAsync(testAddress, TestBytesBuffer.Create(), new SendingMetadata
            {
                Delay = TimeSpan.FromSeconds(1),
                Ttl = TimeSpan.FromMilliseconds(500)
            });

            await Task.Delay(TimeSpan.FromSeconds(2));

            await host.StopAsync();

            // Assert
            Assert.False(resultsChannel.Reader.TryRead(out _));
        }
    }
}
