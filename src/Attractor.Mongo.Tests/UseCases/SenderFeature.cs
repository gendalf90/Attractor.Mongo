using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;
using System.Collections.Generic;
using MongoDB.Driver;
using System;

namespace Attractor.Mongo.Tests.UseCases
{
    public class SenderFeature
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
                    services.AddTractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var sender = context.Metadata.GetFeature<ISenderFeature>();
                        var self = context.Metadata.GetFeature<ISelfFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        if (sender == null)
                        {
                            await self.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/2"), TestBytesBuffer.Create());
                        }
                        else
                        {
                            await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString(sender));
                        }

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

            var senderAddress = await resultsChannel.Reader.ReadAsync();

            await host.StopAsync();

            // Assert
            Assert.Equal($"{testAddress}/1", senderAddress);
        }

        [Fact]
        public async Task SendMessageToSender()
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
                        var sender = context.Metadata.GetFeature<ISenderFeature>();
                        var self = context.Metadata.GetFeature<ISelfFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        if (sender == null)
                        {
                            await self.SendMessageAsync(TestBytesBuffer.CreateString($"{testAddress}/2"), TestBytesBuffer.Create());
                        }
                        else if (TestBytesBuffer.CreateAddress(message).IsString($"{testAddress}/2"))
                        {
                            await sender.SendMessageAsync(TestBytesBuffer.CreateString("payload to response a sender"));
                        }
                        else
                        {
                            await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IAddress)message));
                            await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString((IPayload)message));
                        }

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

            for (int i = 0; i < 2; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Contains($"{testAddress}/1", results);
            Assert.Contains("payload to response a sender", results);
        }
    }
}
