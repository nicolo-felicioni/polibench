package cassandra;

import com.datastax.driver.core.*;
import common.Database;
import common.Operation;
import common.data.Data;
import common.data.entity.Engagement;
import common.data.entity.Tweet;
import common.data.entity.User;
import common.operation.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NewCassandraDatabase implements Database {

    private CassandraConfiguration configuration;
    private Cluster cluster;
    private Session session;

    public NewCassandraDatabase(CassandraConfiguration configuration) {
        this.configuration = configuration;
    }

    public Session getSession() {
        return this.session;
    }

    @Override
    public void connect() {
        String node = configuration.getAddress();
        Integer port = configuration.getPort();
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
        session = cluster.connect();
    }

    public void close() {
        session.close();
        cluster.close();
    }


    @Override
    public void execute(Operation operation) {
        operation.visit(this);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean executeOperation(WriteOperation operation) {
        BoundStatement statement;
        // Creator
        User creator = operation.getDataRow().getCreator();
        PreparedStatement creatorPreparedStatement =
                session.prepare("insert into usde.user (id,verified) values (?, ?)");
        statement = creatorPreparedStatement.bind(creator.getUserId(), creator.getVerified());
        session.execute(statement);
        // Engager
        User engager = operation.getDataRow().getEngager();
        PreparedStatement engagerPreparedStatement =
                session.prepare("insert into usde.user (id,verified) values (?, ?)");
        statement = engagerPreparedStatement.bind(engager.getUserId(), engager.getVerified());
        session.execute(statement);
        // Tweet
        Tweet tweet = operation.getDataRow().getTweet();
        PreparedStatement tweetPreparedStatement =
                session.prepare("insert into usde.tweet (id,language,creation_timestamp,creator) values (?, ?, ?, ?)");
        statement = tweetPreparedStatement.bind(
                tweet.getTweetId(),
                tweet.getLanguage(),
                tweet.getCreationTimestamp(),
                creator.getUserId());
        session.execute(statement);
        // Engagement
        Engagement engagement = operation.getDataRow().getEngagement();
        PreparedStatement engagementPreparedStatement =
                session.prepare("insert into usde.engagement (tweet_id,engager_id,engagement)  values (?, ?, ?)");
        statement = engagementPreparedStatement.bind(
                tweet.getTweetId(),
                engager.getUserId(),
                engagement.getEngagement());
        session.execute(statement);
        // Join table
        PreparedStatement joinPreparedStatement =
                session.prepare("insert into usde.row (tweet_id,engager_id,creator_id,verified,language,creation_timestamp,engagement)  values (?, ?, ?, ?, ?, ?, ?)");
        statement = joinPreparedStatement.bind(
                tweet.getTweetId(),
                engager.getUserId(),
                creator.getUserId(),
                creator.getVerified(),
                tweet.getLanguage(),
                tweet.getCreationTimestamp(),
                engagement.getEngagement());
        session.execute(statement);
        // Commit the transaction
        return true;
    }

    @Override
    public Boolean executeOperation(BatchWriteOperation operation) {
        BatchStatement batch = new BatchStatement();
        operation.getDataRowList().forEach(row -> {
            BoundStatement statement;
            // Creator
            User creator = row.getCreator();
            PreparedStatement creatorPreparedStatement =
                    session.prepare("insert into usde.user (id,verified) values (?, ?)");
            statement = creatorPreparedStatement.bind(creator.getUserId(), creator.getVerified());
            batch.add(statement);
            // Engager
            User engager = row.getEngager();
            PreparedStatement engagerPreparedStatement =
                    session.prepare("insert into usde.user (id,verified) values (?, ?)");
            statement = engagerPreparedStatement.bind(engager.getUserId(), engager.getVerified());
            batch.add(statement);
            // Tweet
            Tweet tweet = row.getTweet();
            PreparedStatement tweetPreparedStatement =
                    session.prepare("insert into usde.tweet (id,language,creation_timestamp,creator) values (?, ?, ?, ?)");
            statement = tweetPreparedStatement.bind(
                    tweet.getTweetId(),
                    tweet.getLanguage(),
                    tweet.getCreationTimestamp(),
                    creator.getUserId());
            batch.add(statement);
            // Engagement
            Engagement engagement = row.getEngagement();
            PreparedStatement engagementPreparedStatement =
                    session.prepare("insert into usde.engagement (tweet_id,engager_id,engagement)  values (?, ?, ?)");
            statement = engagementPreparedStatement.bind(
                    tweet.getTweetId(),
                    engager.getUserId(),
                    engagement.getEngagement());
            batch.add(statement);
            // Join
            PreparedStatement joinPreparedStatement =
                    session.prepare("insert into usde.row (tweet_id,engager_id,creator_id,verified,language,creation_timestamp,engagement)  values (?, ?, ?, ?, ?, ?, ?)");
            statement = joinPreparedStatement.bind(
                    tweet.getTweetId(),
                    engager.getUserId(),
                    creator.getUserId(),
                    creator.getVerified(),
                    tweet.getLanguage(),
                    tweet.getCreationTimestamp(),
                    engagement.getEngagement());
            batch.add(statement);
        });
        session.execute(batch);
        return true;
    }

    @Override
    public Tweet executeOperation(GetTweetByIdOperation operation) {
        String id = operation.getTweetId();
        PreparedStatement tweetPreparedStatement =
                session.prepare("select * from usde.tweet where id = ? ALLOW FILTERING");
        BoundStatement statement = tweetPreparedStatement.bind(id);
        ResultSet resultSet = session.execute(statement);
        Row row = resultSet.one();
        if (row != null) {
            Tweet tweet = new Tweet(
                    row.getString("id"),
                    row.getInt("creation_timestamp"),
                    row.getString("language")
            );
            return tweet;
        }
        return null;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByLanguageOperation operation) {
        List<Tweet> result = new ArrayList<>();
        String language = operation.getLanguage();
        PreparedStatement tweetPreparedStatement =
                session.prepare("select * from usde.tweet where language = ? ALLOW FILTERING");
        BoundStatement statement = tweetPreparedStatement.bind(language);
        ResultSet resultSet = session.execute(statement);
        result = resultSet.all().stream().map(row -> new Tweet(
                row.getString("id"),
                row.getInt("creation_timestamp"),
                row.getString("language")
        )).collect(Collectors.toList());
        return result;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByTimestampIntervalOperation operation) {
        List<Tweet> result = new ArrayList<>();
        Integer startTime = operation.getStartTime();
        Integer endTime = operation.getEndTime();
        PreparedStatement tweetPreparedStatement =
                session.prepare("select * from usde.tweet where creation_timestamp > ? and creation_timestamp < ? ALLOW FILTERING");
        BoundStatement statement = tweetPreparedStatement.bind(startTime, endTime);
        ResultSet resultSet = session.execute(statement);
        result = resultSet.all().stream().map(row -> new Tweet(
                row.getString("id"),
                row.getInt("creation_timestamp"),
                row.getString("language")
        )).collect(Collectors.toList());
        return result;
    }

    @Override
    public Map<String, Integer> executeOperation(GetCountEngagementGroupByLanguageOperation operation) {
        Map<String, Integer> result = new HashMap<>();
        PreparedStatement statement =
                session.prepare("select language, count(*) as count from usde.row group by language ALLOW FILTERING");

        ResultSet resultSet = session.execute(statement.bind());
        resultSet.all().forEach(row -> {
            String language = row.getString("language");
            Integer count = (int) row.getLong("count");
            result.put(language, count);
        });
        return result;
    }

    public static void main(String[] args) throws Exception{
        CassandraConfiguration conf = new CassandraConfiguration("151.0.231.141", 9042, "usde", 3, "password");
        CassandraDatabase database = new CassandraDatabase(conf);
        database.connect();
        Data data = new Data();
        data.readFromFile("train_days_1.csv.gz");
        data.getRowStream().forEach(d -> database.execute(new WriteOperation(d)));
        database.execute(new BatchWriteOperation(data.getRowStream().collect(Collectors.toList())));
        database.execute(new GetTweetByIdOperation("193D2149C79584C55BFE054B52A96D38"));
        database.execute(new GetTweetByLanguageOperation("06D61DCBBE938971E1EA0C38BD9B5446"));
        database.execute(new GetTweetByTimestampIntervalOperation(1580968766, 1580978766));
        database.execute(new GetCountEngagementGroupByLanguageOperation());
        database.close();
    }
}
