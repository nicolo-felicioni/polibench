package mongodb;


import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import common.Database;
import common.Operation;
import common.data.Data;
import common.data.entity.Tweet;
import common.operation.*;
import mongodb.data.MongoDataRow;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Accumulators.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.group;

public class MongoDatabase implements Database {

    private MongoClient mongoClient;
    private final MongoConfiguration configuration;
    private com.mongodb.client.MongoDatabase database;

    public MongoDatabase(MongoConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void connect() {

        // If the MongoDB needs an authentication
        if (configuration.getUseAuthentication()) {
            MongoCredential credential = MongoCredential.createPlainCredential(
                    configuration.getUsername(),
                    configuration.getDbName(),
                    configuration.getPassword().toCharArray()
            );
            List<MongoCredential> auths = new ArrayList<MongoCredential>();
            auths.add(credential);
            ServerAddress serverAddress = new ServerAddress(configuration.getAddress(), configuration.getPort());
            this.mongoClient = new MongoClient(serverAddress, auths);
        } else {
            ServerAddress serverAddress = new ServerAddress(configuration.getAddress(), configuration.getPort());
            this.mongoClient = new MongoClient(serverAddress);
        }
        // Retrieve the database object
        this.database = mongoClient.getDatabase(configuration.getDbName());

        // Add codec for POJOs
        CodecRegistry defaultCodecRegistry = MongoClientSettings.getDefaultCodecRegistry();
        CodecRegistry fromProvider = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(defaultCodecRegistry, fromProvider);
        this.database = database.withCodecRegistry(pojoCodecRegistry);
    }

    @Override
    public void close() {
        this.mongoClient.close();
    }

    @Override
    public void execute(Operation operation) {
        operation.visit(this);
    }

    @Override
    public void clear() {
         // Reset the collection
         this.database.getCollection(configuration.getCollectionName()).drop();
    }

    @Override
    public Boolean executeOperation(WriteOperation operation) {
        MongoCollection<MongoDataRow> collection = database.getCollection(configuration.getCollectionName(), MongoDataRow.class);
        collection.insertOne(new MongoDataRow(operation.getDataRow()));
        return true;
    }

    @Override
    public Boolean executeOperation(BatchWriteOperation operation) {
        MongoCollection<MongoDataRow> collection = database.getCollection(configuration.getCollectionName(), MongoDataRow.class);
        collection.insertMany(operation.getDataRowList().stream().map(MongoDataRow::new).collect(Collectors.toList()));
        return true;
    }

    @Override
    public Tweet executeOperation(GetTweetByIdOperation operation) {
        MongoCollection<MongoDataRow> collection = database.getCollection(configuration.getCollectionName(), MongoDataRow.class);
        BasicDBObject query = new BasicDBObject("tweet.tweetId", operation.getTweetId());
        MongoDataRow queryResult = collection.find(query).first();
        return queryResult == null ? null : queryResult.getTweet();
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByLanguageOperation operation) {
        MongoCollection<MongoDataRow> collection = database.getCollection(configuration.getCollectionName(), MongoDataRow.class);
        BasicDBObject query = new BasicDBObject("tweet.language", operation.getLanguage());
        List<Tweet> result = new ArrayList<>();
        for (MongoDataRow data : collection.find(query)) {
            result.add(data.getTweet());
        }
        return result;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByTimestampIntervalOperation operation) {
        MongoCollection<MongoDataRow> collection = database.getCollection(configuration.getCollectionName(), MongoDataRow.class);
        Integer start = operation.getStartTime();
        Integer end = operation.getEndTime();
        BasicDBObject query = new BasicDBObject("tweet.creationTimestamp", new BasicDBObject("$gt", start).append("$lte", end));
        List<Tweet> result = new ArrayList<>();
        for (MongoDataRow data : collection.find(query)) {
            result.add(data.getTweet());
        }
        return result;
    }

    @Override
    public Map<String, Integer> executeOperation(GetCountEngagementGroupByLanguageOperation operation) {
        MongoCollection collection = database.getCollection(configuration.getCollectionName());
        Bson groupby = group("$tweet.language", sum("count", 1));
        AggregateIterable<Document> result = collection.aggregate(Collections.singletonList(groupby), Document.class);
        Map<String, Integer> map = new HashMap<>();
        for (Document d : result) {
            String id = (String) d.get("_id");
            Integer count = (Integer) d.get("count");
            map.put(id, count);
        }
        return map;
    }


    public static void main(String[] args) throws IOException {
        MongoConfiguration conf = new MongoConfiguration("151.0.231.141", 27017, "benchmark", Boolean.FALSE, "user", "password", "benchmark_collection");
        MongoDatabase database = new MongoDatabase(conf);
        database.connect();
        database.clear();
        Data data = new Data();
        data.readFromFile("train_days_1.csv.gz");
        data.getRowStream().forEach(d -> database.execute(new WriteOperation(d)));
        database.execute(new BatchWriteOperation(data.getRowStream().collect(Collectors.toList())));
        database.execute(new GetTweetByIdOperation("193D2149C79584C55BFE054B52A96D38"));
        database.execute(new GetTweetByLanguageOperation("06D61DCBBE938971E1EA0C38BD9B5446"));
        database.execute(new GetTweetByTimestampIntervalOperation(1580968766, 1580978766));
        database.execute(new GetCountEngagementGroupByLanguageOperation());
    }
}
