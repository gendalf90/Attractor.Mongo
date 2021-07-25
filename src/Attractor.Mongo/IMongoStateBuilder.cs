namespace Attractor.Mongo
{
    public interface IMongoStateBuilder
    {
        void UseDatabaseName(string name);

        void UseCollectionName(string name);
    }
}
