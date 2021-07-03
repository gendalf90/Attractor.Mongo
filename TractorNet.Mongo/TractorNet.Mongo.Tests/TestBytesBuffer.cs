using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TractorNet.Mongo.Tests
{
    public class TestBytesBuffer : IAddress, IPayload, IAddressPolicy
    {
        public readonly ReadOnlyMemory<byte> bytes;

        public TestBytesBuffer(ReadOnlyMemory<byte> bytes)
        {
            this.bytes = bytes;
        }

        public ReadOnlyMemory<byte> GetBytes()
        {
            return bytes;
        }

        public ValueTask<bool> IsMatchAsync(IAddress address, CancellationToken token = default)
        {
            return ValueTask.FromResult(bytes.Span.SequenceEqual(address.GetBytes().Span));
        }

        public bool IsString(string str)
        {
            return Encoding.UTF8.GetBytes(str).AsSpan().SequenceEqual(bytes.Span);
        }

        public static TestBytesBuffer Create(params byte[] bytes)
        {
            return new TestBytesBuffer(bytes);
        }

        public static TestBytesBuffer CreateAddress(IAddress address)
        {
            return new TestBytesBuffer(address.GetBytes());
        }

        public static TestBytesBuffer CreatePayload(IPayload payload)
        {
            return new TestBytesBuffer(payload.GetBytes());
        }

        public static TestBytesBuffer CreateString(string str)
        {
            return new TestBytesBuffer(Encoding.UTF8.GetBytes(str));
        }
    }
}
