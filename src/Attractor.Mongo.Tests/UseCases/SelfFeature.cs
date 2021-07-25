using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using System;

namespace Attractor.Mongo.Tests.UseCases
{
    public class SelfFeature
    {
        [Fact]
        public async Task UseAddress()
        {
            // Arrange
            var testAddress = Guid.NewGuid().ToString();
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddAttractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var self = context.Metadata.GetFeature<ISelfFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString(self));
                        await message.ConsumeAsync();
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

            await outbox.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/1"), TestBytesBuffer.Create());
            await outbox.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/2"), TestBytesBuffer.Create());
            await outbox.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/3"), TestBytesBuffer.Create());

            var results = new List<string>();

            for (int i = 0; i < 3; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Contains($"{testAddress}/1", results);
            Assert.Contains($"{testAddress}/2", results);
            Assert.Contains($"{testAddress}/3", results);
        }

        [Fact]
        public async Task SendMessageToOtherActor()
        {
            // Arrange
            var testAddress = Guid.NewGuid().ToString();
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddAttractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var self = context.Metadata.GetFeature<ISelfFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        if (TestBytesBuffer.CreateAddress(self).IsString($"{testAddress}/1"))
                        {
                            await self.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/2"), TestBytesBuffer.CreateString("payload/2"));
                        }

                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IAddress)message));
                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IPayload)message));
                        await message.ConsumeAsync();
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

            var results = new List<string>();

            for (int i = 0; i < 4; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Contains($"{testAddress}/1", results);
            Assert.Contains($"{testAddress}/2", results);
            Assert.Contains("payload/1", results);
            Assert.Contains("payload/2", results);
        }

        [Fact]
        public async Task SendMessageToSelfActor()
        {
            // Arrange
            var testAddress = Guid.NewGuid().ToString();
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddAttractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var self = context.Metadata.GetFeature<ISelfFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        if (TestBytesBuffer.CreatePayload(message).IsString("payload/1"))
                        {
                            await self.SendMessageAsync(TestBytesBuffer.CreateString("payload/2"));
                        }

                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IAddress)message));
                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IPayload)message));
                        await message.ConsumeAsync();
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

            var results = new List<string>();

            for (int i = 0; i < 4; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Equal(2, results.Count(result => result == $"{testAddress}/1"));
            Assert.Contains("payload/1", results);
            Assert.Contains("payload/2", results);
        }
    }
}
