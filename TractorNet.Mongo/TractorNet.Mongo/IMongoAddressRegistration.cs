using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo
{
    internal interface IMongoAddressRegistration
    {
        ValueTask ProlongAddressUsingAsync(IAddress address, CancellationToken token = default);
    }
}
