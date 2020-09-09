package mysql;

import common.Database;
import common.Operation;
import common.data.Data;
import common.data.entity.Engagement;
import common.data.entity.Tweet;
import common.data.entity.User;
import common.operation.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLDatabase implements Database {

    private MySQLConfiguration configuration;
    private Connection connection;

    public MySQLDatabase(MySQLConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void connect() {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            this.connection = DriverManager
                    .getConnection(
                            "jdbc:mysql://" + configuration.getAddress()
                                    + ":" + configuration.getPort()
                                    + "/" + configuration.getDbName() + "?user="
                                    + configuration.getUsername() + "&password="
                                    + configuration.getPassword()
                    );

            // Set auto commit to false for better management of transactions
            this.connection.setAutoCommit(false);
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void execute(Operation operation) {
        operation.visit(this);
    }

    @Override
    public void clear() {
        try {
            // Delete tweets
            PreparedStatement statement = connection
                    .prepareStatement("delete from tweet");
            statement.executeUpdate();
            // Delete users
            statement = connection
                    .prepareStatement("delete from user");
            statement.executeUpdate();
            // Delete tweets
            statement = connection
                    .prepareStatement("delete from engagement");
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public Boolean executeOperation(WriteOperation operation) {
        try {
            // Creator
            User creator = operation.getDataRow().getCreator();
            PreparedStatement creatorPreparedStatement = connection
                    .prepareStatement("insert ignore into user values (?, ?)");
            creatorPreparedStatement.setString(1, creator.getUserId());
            creatorPreparedStatement.setBoolean(2, creator.getVerified());
            creatorPreparedStatement.executeUpdate();
            // Tweet
            Tweet tweet = operation.getDataRow().getTweet();
            PreparedStatement tweetPreparedStatement = connection
                    .prepareStatement("insert ignore into tweet values (?, ?, ?, ?)");
            tweetPreparedStatement.setString(1, tweet.getTweetId());
            tweetPreparedStatement.setString(2, tweet.getLanguage());
            tweetPreparedStatement.setLong(3, tweet.getCreationTimestamp());
            tweetPreparedStatement.setString(4, creator.getUserId());
            tweetPreparedStatement.executeUpdate();
            // Engager
            User engager = operation.getDataRow().getEngager();
            PreparedStatement engagerPreparedStatement = connection
                    .prepareStatement("insert ignore into user values (?, ?)");
            engagerPreparedStatement.setString(1, engager.getUserId());
            engagerPreparedStatement.setBoolean(2, engager.getVerified());
            engagerPreparedStatement.executeUpdate();
            // Engagement
            Engagement engagement = operation.getDataRow().getEngagement();
            PreparedStatement engagementPreparedStatement = connection
                    .prepareStatement("insert ignore into engagement values (?, ?, ?)");
            engagementPreparedStatement.setString(1, tweet.getTweetId());
            engagementPreparedStatement.setString(2, engager.getUserId());
            engagementPreparedStatement.setBoolean(3, engagement.getEngagement());
            engagementPreparedStatement.executeUpdate();
            // Commit the transaction
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return true;
    }

    @Override
    public Boolean executeOperation(BatchWriteOperation operation) {
        try {
            PreparedStatement creatorPreparedStatement = connection
                    .prepareStatement("insert ignore into user values (?, ?)");
            PreparedStatement tweetPreparedStatement = connection
                    .prepareStatement("insert ignore into tweet values (?, ?, ?, ?)");
            PreparedStatement engagerPreparedStatement = connection
                    .prepareStatement("insert ignore into user values (?, ?)");
            PreparedStatement engagementPreparedStatement = connection
                    .prepareStatement("insert ignore into engagement values (?, ?, ?)");
            operation.getDataRowList().forEach(row -> {
                try {
                    // Creator
                    User creator = row.getCreator();
                    creatorPreparedStatement.setString(1, creator.getUserId());
                    creatorPreparedStatement.setBoolean(2, creator.getVerified());
                    creatorPreparedStatement.addBatch();
                    // Tweet
                    Tweet tweet = row.getTweet();
                    tweetPreparedStatement.setString(1, tweet.getTweetId());
                    tweetPreparedStatement.setString(2, tweet.getLanguage());
                    tweetPreparedStatement.setLong(3, tweet.getCreationTimestamp());
                    tweetPreparedStatement.setString(4, creator.getUserId());
                    tweetPreparedStatement.addBatch();
                    // Engager
                    User engager = row.getEngager();
                    engagerPreparedStatement.setString(1, engager.getUserId());
                    engagerPreparedStatement.setBoolean(2, engager.getVerified());
                    engagerPreparedStatement.addBatch();
                    // Engagement
                    Engagement engagement = row.getEngagement();
                    engagementPreparedStatement.setString(1, tweet.getTweetId());
                    engagementPreparedStatement.setString(2, engager.getUserId());
                    engagementPreparedStatement.setBoolean(3, engagement.getEngagement());
                    engagementPreparedStatement.addBatch();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            });
            // Commit the transaction
            creatorPreparedStatement.executeBatch();
            tweetPreparedStatement.executeBatch();
            engagerPreparedStatement.executeBatch();
            engagementPreparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return true;
    }

    @Override
    public Tweet executeOperation(GetTweetByIdOperation operation) {
        try {
            PreparedStatement statement = connection
                    .prepareStatement("select * from tweet where id = ?");
            statement.setString(1, operation.getTweetId());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                Tweet tweet = new Tweet(
                        resultSet.getString("id"),
                        resultSet.getInt("creation_timestamp"),
                        resultSet.getString("language")
                );
                return tweet;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByLanguageOperation operation) {
        List<Tweet> result = new ArrayList<>();
        try {
            PreparedStatement statement = connection
                    .prepareStatement("select * from tweet where language = ?");
            statement.setString(1, operation.getLanguage());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Tweet tweet = new Tweet(
                        resultSet.getString("id"),
                        resultSet.getInt("creation_timestamp"),
                        resultSet.getString("language")
                );
                result.add(tweet);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return result;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByTimestampIntervalOperation operation) {
        List<Tweet> result = new ArrayList<>();
        try {
            PreparedStatement statement = connection
                    .prepareStatement("select * from tweet where creation_timestamp between ? and ?");
            statement.setInt(1, operation.getStartTime());
            statement.setInt(2, operation.getEndTime());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Tweet tweet = new Tweet(
                        resultSet.getString("id"),
                        resultSet.getInt("creation_timestamp"),
                        resultSet.getString("language")
                );
                result.add(tweet);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return result;
    }

    @Override
    public Map<String, Integer> executeOperation(GetCountEngagementGroupByLanguageOperation operation) {
        Map<String, Integer> result = new HashMap<>();
        try {
            PreparedStatement statement = connection
                    .prepareStatement("select t.language as language, count(*) as count from tweet t inner join engagement e on t.id = e.tweet_id group by t.language");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String language = resultSet.getString("language");
                Integer count = resultSet.getInt("count");
                result.put(language, count);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
//        MySQLConfiguration conf = new MySQLConfiguration("151.0.231.141", 3306, "benchmark_mysql", "root", "password");
////        MySQLConfiguration conf = new MySQLConfiguration("localhost", 15306, "benchmark_vitess", "root", "password");
//        MySQLDatabase database = new MySQLDatabase(conf);
//        database.connect();
//        database.clear();
//        Data data = new Data();
//        data.readFromFile("train_days_1.csv.gz");
//        data.getRowStream().forEach(d -> database.execute(new WriteOperation(d)));
//        database.execute(new BatchWriteOperation(data.getRowStream().collect(Collectors.toList())));
//        database.execute(new GetTweetByIdOperation("193D2149C79584C55BFE054B52A96D38"));
//        database.execute(new GetTweetByLanguageOperation("06D61DCBBE938971E1EA0C38BD9B5446"));
//        database.execute(new GetTweetByTimestampIntervalOperation(1580968766, 1580978766));
//        database.execute(new GetCountEngagementGroupByLanguageOperation());

        MySQLConfiguration conf = new MySQLConfiguration("localhost", 15306, "benchmark_vitess", "root", "password");
        MySQLDatabase database = new MySQLDatabase(conf);
        database.connect();
        database.clear();
        Data data = new Data();
        data.readFromFile("train_days_1.csv.gz");
        data.getRowStream().limit(10).forEach(d -> database.execute(new WriteOperation(d)));
    }
}
