using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo
{
    public interface IMongoAddressFeature
    {
        ValueTask ProlongAddressUsingAsync(CancellationToken token = default);
    }
}
