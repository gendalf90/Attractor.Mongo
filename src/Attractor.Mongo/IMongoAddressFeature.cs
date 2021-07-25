using System.Threading;
using System.Threading.Tasks;

namespace Attractor.Mongo
{
    public interface IMongoAddressFeature
    {
        ValueTask ProlongAddressUsingAsync(CancellationToken token = default);
    }
}
