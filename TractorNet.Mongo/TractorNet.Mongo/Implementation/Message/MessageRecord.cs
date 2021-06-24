﻿using MongoDB.Bson;
using System;

namespace TractorNet.Mongo.Implementation.Message
{
    internal sealed class MessageRecord
    {
        public ObjectId Id { get; set; }

        public byte[] From { get; set; }

        public byte[] To { get; set; }

        public byte[] Payload { get; set; }

        public DateTime? ExpireAt { get; set; }

        public DateTime AvailableAt { get; set; }

        public DateTime UnlockedAt { get; set; }
    }
}