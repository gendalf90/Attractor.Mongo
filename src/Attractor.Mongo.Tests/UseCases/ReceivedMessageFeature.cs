using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;

namespace Attractor.Mongo.Tests.UseCases
{
    public class ReceivedMessageFeature
    {
        [Fact]
        public async Task UseAddressAndPayload()
        {
            // Arrange
            var testAddress = Guid.NewGuid().ToString();
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var feature = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IAddress)feature));
                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IPayload)feature));

                        await feature.ConsumeAsync();
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy((address, token) => TestBytesBuffer.GetString(address).StartsWith(testAddress));
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            await outbox.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/1"), TestBytesBuffer.CreateString("payload/1"));
            await outbox.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/2"), TestBytesBuffer.CreateString("payload/2"));
            await outbox.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/3"), TestBytesBuffer.CreateString("payload/3"));

            var results = new List<string>();

            for (int i = 0; i < 6; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Contains($"{testAddress}/1", results);
            Assert.Contains($"{testAddress}/2", results);
            Assert.Contains($"{testAddress}/3", results);
            Assert.Contains("payload/1", results);
            Assert.Contains("payload/2", results);
            Assert.Contains("payload/3", results);
        }

        [Fact]
        public async Task RunWithoutConsumingMessage()
        {
            // Arrange
            var testAddress = TestBytesBuffer.Generate();
            var receivedCount = 0;
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var feature = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IPayload)feature));

                        if (++receivedCount == 3)
                        {
                            await feature.ConsumeAsync();
                        }
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

            await outbox.SendMessageAsync(testAddress, TestBytesBuffer.CreateString("payload"));

            var results = new List<string>();

            for (int i = 0; i < 3; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.All(results, result => Assert.Equal("payload", result));
        }

        [Fact]
        public async Task RunWithDelayingMessage()
        {
            // Arrange
            var testAddress = TestBytesBuffer.Generate();
            var isReceivedAtFirstTime = true;
            var resultsChannel = Channel.CreateUnbounded<DateTime>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var feature = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultsChannel.Writer.WriteAsync(DateTime.UtcNow);

                        if (isReceivedAtFirstTime)
                        {
                            await feature.DelayAsync(TimeSpan.FromSeconds(1));

                            isReceivedAtFirstTime = false;
                        }
                        else
                        {
                            await feature.ConsumeAsync();
                        }
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

            await outbox.SendMessageAsync(testAddress, TestBytesBuffer.Create());

            var timeOfFirstReceiving = await resultsChannel.Reader.ReadAsync();
            var timeOfSecondReceiving = await resultsChannel.Reader.ReadAsync();

            await host.StopAsync();

            // Assert
            Assert.True(timeOfSecondReceiving - timeOfFirstReceiving >= TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task RunWithExpiringMessage()
        {
            // Arrange
            var testAddress = TestBytesBuffer.Generate();
            var isReceivedAtFirstTime = true;
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var feature = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        if (isReceivedAtFirstTime)
                        {
                            await feature.ExpireAsync(TimeSpan.FromMilliseconds(500));
                            await feature.DelayAsync(TimeSpan.FromSeconds(1));

                            isReceivedAtFirstTime = false;
                        }
                        else
                        {
                            await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IPayload)feature));
                        }
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

            await outbox.SendMessageAsync(testAddress, TestBytesBuffer.CreateString("payload"));

            await Task.Delay(TimeSpan.FromSeconds(2));

            await host.StopAsync();

            // Assert
            Assert.False(resultsChannel.Reader.TryRead(out _));
        }
    }
}
