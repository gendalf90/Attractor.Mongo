using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Xunit;

namespace TractorNet.Mongo.Tests.UseCases
{
    public class Registration
    {
        [Fact]
        public async Task Run()
        {
            // Arrange
            var resultChannel = Channel.CreateUnbounded<IMessage>();

            using var host = new HostBuilder()
                .ConfigureServices(services =>
                {
                    services.AddTractor();
                    services.RegisterActor(async (context, token) =>
                    {
                        var feature = context.Metadata.GetFeature<IReceivedMessageFeature>();

                        await resultChannel.Writer.WriteAsync(feature, token);
                        await feature.ConsumeAsync(token);
                    }, actorBuilder =>
                    {
                        actorBuilder.UseAddressPolicy(_ => TestBytesBuffer.CreateString("address"));
                    });
                    services.UseMongoAddressBook(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString), builder =>
                    {
                        // "tractor" by default
                        builder.UseDatabaseName("testAddressDb");

                        // "addressBook" by default
                        builder.UseCollectionName("testAddresses");

                        // needed for serializing bytes of the address to string
                        // so the type of the field in mongo will be the serializer returns (by default the type is binData)
                        builder.UseAddressSerializer(new TestStringSerializer());
                    });
                    services.UseMongoMailbox(MongoClientSettings.FromConnectionString(TestMongoServer.ConnectionString), builder =>
                    {
                        // "tractor" by default
                        builder.UseDatabaseName("testMessageDb");

                        // "mailbox" by default
                        builder.UseCollectionName("testMessages");

                        builder.UseAddressSerializer(new TestStringSerializer());
                        builder.UsePayloadSerializer(new TestStringSerializer());
                    });
                })
                .Build();

            // Act
            await host.StartAsync();

            var outbox = host.Services.GetRequiredService<IAnonymousOutbox>();

            var resultsMessages = new List<IMessage>();

            for (int i = 0; i < 3; i++)
            {
                await outbox.SendMessageAsync(TestBytesBuffer.CreateString("address"), TestBytesBuffer.CreateString($"payload {i}"));

                resultsMessages.Add(await resultChannel.Reader.ReadAsync());
            }

            await host.StopAsync();

            // Assert
            Assert.All<IAddress>(resultsMessages, address =>
            {
                Assert.True(TestBytesBuffer.CreateAddress(address).IsString("address"));
            });
            Assert.Contains<IPayload>(resultsMessages, payload => TestBytesBuffer.CreatePayload(payload).IsString("payload 0"));
            Assert.Contains<IPayload>(resultsMessages, payload => TestBytesBuffer.CreatePayload(payload).IsString("payload 1"));
            Assert.Contains<IPayload>(resultsMessages, payload => TestBytesBuffer.CreatePayload(payload).IsString("payload 2"));
        }
    }
}
