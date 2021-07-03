using MongoDB.Bson.Serialization;
using System;
using System.Text;

namespace TractorNet.Mongo.Tests
{
    public class TestStringSerializer : IBsonSerializer<byte[]>
    {
        public Type ValueType => typeof(byte[]);

        public byte[] Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            return Encoding.UTF8.GetBytes(BsonSerializer.Deserialize<string>(context.Reader));
        }

        public void Serialize(BsonSerializationContext context, BsonSerializationArgs args, byte[] value)
        {
            BsonSerializer.Serialize(context.Writer, Encoding.UTF8.GetString(value));
        }

        void IBsonSerializer.Serialize(BsonSerializationContext context, BsonSerializationArgs args, object value)
        {
            BsonSerializer.Serialize(context.Writer, Encoding.UTF8.GetString((byte[])value));
        }

        object IBsonSerializer.Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            return Encoding.UTF8.GetBytes(BsonSerializer.Deserialize<string>(context.Reader));
        }
    }
}
