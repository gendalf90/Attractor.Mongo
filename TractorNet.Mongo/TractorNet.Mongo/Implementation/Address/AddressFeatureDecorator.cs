using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo.Implementation.Address
{
    internal sealed class AddressFeatureDecorator : IActorDecorator
    {
        private readonly IMongoAddressRegistration addressRegistration;

        private IActor actor;

        public AddressFeatureDecorator(IMongoAddressRegistration addressRegistration)
        {
            this.addressRegistration = addressRegistration;
        }

        public void Decorate(IActor actor)
        {
            this.actor = actor;
        }

        public async ValueTask OnReceiveAsync(ReceivedMessageContext context, CancellationToken token = default)
        {
            TryAddAddressFeature(context);

            await actor.OnReceiveAsync(context, token);
        }

        private void TryAddAddressFeature(ReceivedMessageContext context)
        {
            var receivedMessageFeature = context.Metadata.GetFeature<IReceivedMessageFeature>();

            if (receivedMessageFeature == null)
            {
                return;
            }

            context.Metadata.SetFeature<IMongoAddressFeature>(new AddressFeature(addressRegistration, receivedMessageFeature));
        }

        private class AddressFeature : IMongoAddressFeature
        {
            private readonly IMongoAddressRegistration addressRegistration;
            private readonly IAddress currentAddress;

            public AddressFeature(IMongoAddressRegistration addressRegistration, IAddress currentAddress)
            {
                this.addressRegistration = addressRegistration;
                this.currentAddress = currentAddress;
            }

            public ValueTask ProlongUsingAsync(CancellationToken token = default)
            {
                return addressRegistration.ProlongAddressUsingAsync(currentAddress, token);
            }
        }
    }
}
