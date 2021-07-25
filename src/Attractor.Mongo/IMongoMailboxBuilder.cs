using System;

namespace Attractor.Mongo
{
    public interface IMongoMailboxBuilder
    {
        void UseDatabaseName(string name);

        void UseCollectionName(string name);

        void UseMessageProcessingTimeout(TimeSpan time);

        void UseReadTrottleTime(TimeSpan time);

        void UseReadBatchSize(int size);
    }
}
