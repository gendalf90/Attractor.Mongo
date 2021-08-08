using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;

namespace Attractor.Mongo.Tests.UseCases
{
    public class StateFeature
    {
        [Fact]
        public async Task SaveState()
        {
            // Arrange
            var address = TestBytesBuffer.Generate();
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddAttractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var state = context.Metadata.GetFeature<IStateFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();
                        var currentState = TestBytesBuffer.GetString(state);

                        await resultsChannel.Writer.WriteAsync(currentState);

                        var newState = currentState + "test";

                        await state.SaveAsync(TestBytesBuffer.CreateString(newState));
                        await message.ConsumeAsync();
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy(_ => address);
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoState(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            await outbox.SendMessageAsync(address, TestBytesBuffer.Create());
            await outbox.SendMessageAsync(address, TestBytesBuffer.Create());
            await outbox.SendMessageAsync(address, TestBytesBuffer.Create());

            var results = new List<string>();

            for (int i = 0; i < 3; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Contains("", results);
            Assert.Contains("test", results);
            Assert.Contains("testtest", results);
        }

        [Fact]
        public async Task ClearState()
        {
            // Arrange
            var address = TestBytesBuffer.Generate();
            var isFirstMessage = true;
            var resultsChannel = Channel.CreateUnbounded<string>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddAttractorServer();
                    services.RegisterActor(async (context, token) =>
                    {
                        var state = context.Metadata.GetFeature<IStateFeature>();
                        var message = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultsChannel.Writer.WriteAsync(TestBytesBuffer.GetString(state));

                        if (isFirstMessage)
                        {
                            await state.SaveAsync(TestBytesBuffer.CreateString("test"));
                        }
                        else
                        {
                            await state.ClearAsync();
                        }

                        isFirstMessage = false;

                        await message.ConsumeAsync();
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy(_ => address);
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                    services.UseMongoState(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString));
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            await outbox.SendMessageAsync(address, TestBytesBuffer.Create());
            await outbox.SendMessageAsync(address, TestBytesBuffer.Create());
            await outbox.SendMessageAsync(address, TestBytesBuffer.Create());

            var results = new List<string>();

            for (int i = 0; i < 3; i++)
            {
                results.Add(await resultsChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.Equal(2, results.Count(result => result == ""));
            Assert.Contains("test", results);
        }
    }
}
