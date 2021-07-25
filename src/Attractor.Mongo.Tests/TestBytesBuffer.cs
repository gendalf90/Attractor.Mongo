using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Attractor.Mongo.Tests
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
            return ValueTask.FromResult(IsAddress(address));
        }

        public bool IsAddress(IAddress address)
        {
            return bytes.Span.SequenceEqual(address.GetBytes().Span);
        }

        public bool IsString(string str)
        {
            return Encoding.UTF8.GetString(bytes.Span) == str;
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

        public static string GetString(IAddress address)
        {
            return Encoding.UTF8.GetString(address.GetBytes().Span);
        }

        public static string GetString(IPayload payload)
        {
            return Encoding.UTF8.GetString(payload.GetBytes().Span);
        }

        public static TestBytesBuffer Generate()
        {
            return new TestBytesBuffer(Guid.NewGuid().ToByteArray());
        }
    }
}
