package common;

import common.data.Data;
import common.data.DataRow;
import common.operation.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class Benchmark {

    private final List<Database> databases;
    // Operations
    private List<WriteOperation> writeOperations;
    private List<BatchWriteOperation> batchWriteOperations;
    private List<GetTweetByIdOperation> getTweetByIdOperations;
    private List<GetTweetByLanguageOperation> getTweetByLanguageOperations;
    private List<GetTweetByTimestampIntervalOperation> getTweetByTimestampIntervalOperations;
    private List<GetCountEngagementGroupByLanguageOperation> getCountEngagementGroupByLanguageOperations;

    public static Logger logger = Logger.getLogger(Benchmark.class.getName());

    public Benchmark(List<Database> databases, Data data) {
        this.databases = databases;
        this.initOperations(data);
    }

    public Map<Database, Map<String, SummaryStatistics>> runBenchmark() {
        Map<Database, Map<String, SummaryStatistics>> result = new HashMap<>();
        for (Database database: databases) {
            database.connect();
//            database.clear();
            Map<String, SummaryStatistics> dbResult = new HashMap<>();

            logger.info("Running single writes on " + database.getClass().getSimpleName().toLowerCase());
            dbResult.put("write", runOperations(database, writeOperations));

            logger.info("Running batch writes on " + database.getClass().getSimpleName().toLowerCase());
            dbResult.put("batch_write", runOperations(database, batchWriteOperations));

            logger.info("Running get by id on " + database.getClass().getSimpleName().toLowerCase());
            dbResult.put("get_tweet_by_id", runOperations(database, getTweetByIdOperations));

            logger.info("Running get by language on " + database.getClass().getSimpleName().toLowerCase());
            dbResult.put("get_tweet_by_language", runOperations(database, getTweetByLanguageOperations));

            logger.info("Running get by timestamp on " + database.getClass().getSimpleName().toLowerCase());
            dbResult.put("get_tweet_by_timestamp_interval", runOperations(database, getTweetByTimestampIntervalOperations));

            logger.info("Running get count on " + database.getClass().getSimpleName().toLowerCase());
            dbResult.put("get_engagement_count_group_by_tweet_language", runOperations(database, getCountEngagementGroupByLanguageOperations));
            result.put(database, dbResult);

            database.close();
        }
        return result;
    }

    private SummaryStatistics runOperations(Database database, List<? extends Operation> operations) {
        SummaryStatistics stats = new SummaryStatistics();
        // Warming up
        for (Operation operation: operations.subList(0, operations.size()/2)) {
            database.execute(operation);
        }
        // Benchmark
        for (Operation operation: operations.subList(operations.size()/2, operations.size())) {
            Long startTime = System.currentTimeMillis();
            database.execute(operation);
            Long endTime = System.currentTimeMillis();
            stats.addValue(endTime - startTime);
        }
        return stats;
    }

    private void initOperations(Data data) {
        logger.info("Initializing operations...");
        Random randomSeed = new Random(8);
        // Init write operations
        List<DataRow> dataRows = data.getRowStream().collect(Collectors.toList());
        writeOperations = dataRows
                .subList(0, 1000)
                .stream()
                .map(WriteOperation::new)
                .collect(Collectors.toList());
        // Init batch write operations
        // Split the dataset in multiple batches
        AtomicInteger counter = new AtomicInteger();
        batchWriteOperations = dataRows
                .subList(1000, dataRows.size())
                .stream()
                .collect(Collectors.groupingBy(i -> counter.getAndIncrement() / 100))
                .values()
                .stream()
                .map(BatchWriteOperation::new)
                .collect(Collectors.toList());
        // Init get tweet by id operations
        Collections.shuffle(dataRows, randomSeed);
        getTweetByIdOperations = dataRows
                .subList(0, dataRows.size() / 2)
                .stream()
                .map(dataRow -> new GetTweetByIdOperation(dataRow.getTweet().getTweetId()))
                .collect(Collectors.toList());
        // Init get tweet by language operations
        Collections.shuffle(dataRows, randomSeed);
        getTweetByLanguageOperations = dataRows
                .subList(0, dataRows.size() / 2)
                .stream()
                .map(dataRow -> dataRow.getTweet().getLanguage())
                .distinct()
                .map(GetTweetByLanguageOperation::new)
                .collect(Collectors.toList());
        // Init get tweet by timestamp interval operations
        int min = dataRows.stream().mapToInt(dataRow -> dataRow.getTweet().getCreationTimestamp()).min().orElse(0);
        int max = dataRows.stream().mapToInt(dataRow -> dataRow.getTweet().getCreationTimestamp()).max().orElse(0);
        int step = (max - min) / 30;
        getTweetByTimestampIntervalOperations = IntStream
                .iterate(min, value -> value + step)
                .limit(30)
                .mapToObj(m -> new GetTweetByTimestampIntervalOperation(m, m + step))
                .collect(Collectors.toList());
        // Init get count per language operations
        getCountEngagementGroupByLanguageOperations = IntStream
                .iterate(0, n -> n + 1)
                .limit(10)
                .mapToObj(n -> new GetCountEngagementGroupByLanguageOperation())
                .collect(Collectors.toList());
        logger.info("Operations initialized.");
    }

}

