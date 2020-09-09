package common;

import common.data.entity.Tweet;
import common.operation.*;

import java.util.List;
import java.util.Map;

/**
 * This interface represents an operation executioner.
 */
public interface Executor {

    /**
     * Execute a single write operation.
     * @param operation a WriteOperation containing the data to be written.
     * @return true if the operation has been successful.
     */
    Boolean executeOperation(WriteOperation operation);

    /**
     * Execute a batch write operation.
     * @param operation a BatchWriteOperation containing the data to be written.
     * @return true if the operation has been successful.
     */
    Boolean executeOperation(BatchWriteOperation operation);

    /**
     * Execute a query by tweet id operation.
     * @param operation a GetTweetByIdOperation containing the id of the tweet to be retrieved.
     * @return the queried tweet.
     */
    Tweet executeOperation(GetTweetByIdOperation operation);

    /**
     * Execute a query by tweet language operation.
     * @param operation a GetTweetByLanguageOperation containing the language of the tweets to be retrieved.
     * @return the queried tweets.
     */
    List<Tweet> executeOperation(GetTweetByLanguageOperation operation);

    /**
     * Execute a query by tweet timestamp interval operation.
     * @param operation a GetTweetByLanguageOperation containing the start and the end of the time interval.
     * @return the queried tweets.
     */
    List<Tweet> executeOperation(GetTweetByTimestampIntervalOperation operation);

    Map<String, Integer> executeOperation(GetCountEngagementGroupByLanguageOperation operation);

}
