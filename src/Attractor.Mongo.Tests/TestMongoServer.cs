namespace Attractor.Mongo.Tests
{
    // you can use these commands to start mongodb environment in docker:
    //
    // sudo docker network create mongo-network
    // sudo docker run --name mongo -d --rm -p 27017:27017 --network mongo-network mongo
    // sudo docker run --name mongo-express -d --rm -p 8081:8081 --network mongo-network mongo-express
    // 
    // about mongo-express: (https://hub.docker.com/_/mongo-express)
    public static class TestMongoServer
    {
        // there must be your test mongo server address
        public static string ConnectionString => "mongodb://192.168.61.128:27017";
    }
}
