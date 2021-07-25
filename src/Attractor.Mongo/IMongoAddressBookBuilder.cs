using System;

namespace Attractor.Mongo
{
    public interface IMongoAddressBookBuilder
    {
        void UseDatabaseName(string name);

        void UseCollectionName(string name);

        void UseAddressUsingTimeout(TimeSpan time);
    }
}
