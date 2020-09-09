package voltdb;

import common.Database;
import common.Operation;
import common.data.DataRow;
import common.data.entity.Tweet;
import common.operation.*;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.*;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VoltDB implements Database {

    private Client client = null;
    private final VoltDBConfiguration voltConfiguration;

    public VoltDB(VoltDBConfiguration configuration) {
        this.voltConfiguration = configuration;
    }

    @Override
    public void connect() {

        try {
            client = ClientFactory.createClient(this.voltConfiguration.getConfiguration());
            client.createConnection(this.voltConfiguration.getAddress(), this.voltConfiguration.getPort());
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public void close() {
        try {
            client.drain();
            client.close();
        } catch (NoConnectionsException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Operation operation) {
        operation.visit(this);
    }

    @Override
    public void clear() {
        //throw new UnsupportedOperationException();
    }

    @Override
    public Boolean executeOperation(WriteOperation operation) {
        // taking the parameter of the stored procedure
        DataRow dataRow = operation.getDataRow();
        String creatorUserId = dataRow.getCreator().getUserId();
        int creatorVerified = dataRow.getCreator().getVerified() ? 1 : 0;
        String engagerUserId = dataRow.getEngager().getUserId();
        int engagerVerified = dataRow.getEngager().getVerified() ? 1 : 0;
        String tweetId = dataRow.getTweet().getTweetId();
        String language = dataRow.getTweet().getLanguage();
        long creationTimestamp = dataRow.getTweet().getCreationTimestamp();
        int isPositive = dataRow.getEngagement().getEngagement() ? 1 : 0;

        if(this.voltConfiguration.isMultipleInsertsInSingleProcedure()) {
            System.out.println("Executing multiple inserts in single procedure.");
            multipleInsertsInOneProcedure(creatorUserId, creatorVerified, engagerUserId,
                    engagerVerified, tweetId, language, creationTimestamp, isPositive);
        }
        else {
            System.out.println("Executing multiple inserts in multiple procedures.");
            multipleInsertsInMultipleProcedures(creatorUserId, creatorVerified, engagerUserId,
                    engagerVerified, tweetId, language, creationTimestamp, isPositive);
        }

        return true;
    }

    /*
    * Two possible ways of executing the single write operation.
    * multipleInsertsInMultipleProcedures: the four writes are executed in four different stored procedures.
    *
    *
    * multipleInsertsInOneProcedure: the four writes are executed in a single stored procedure.
    * The latter has the problem that in case of duplicates in a single write, the whole transaction aborts,
    * thus no writes are executed. This may be not intended.
    * */

    private void multipleInsertsInMultipleProcedures(String creatorUserId, int creatorVerified, String engagerUserId, int engagerVerified,
                                                     String tweetId, String language, long creationTimestamp, int isPositive){

        try {
            client.callProcedure("InsertEngagement", tweetId, engagerUserId, isPositive);
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }

        try {
            client.callProcedure("InsertTweet", tweetId, language, creationTimestamp, creatorUserId);
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }

        try {
            client.callProcedure("InsertUser", creatorUserId, creatorVerified);
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }

        try {
            client.callProcedure("InsertUser", engagerUserId, engagerVerified);
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }


    }

    private void multipleInsertsInOneProcedure(String creatorUserId, int creatorVerified, String engagerUserId, int engagerVerified,
                                               String tweetId, String language, long creationTimestamp, int isPositive){
        try {
            client.callProcedure("SingleWriteProcedure", creatorUserId, creatorVerified, engagerUserId,
                    engagerVerified, tweetId, language, creationTimestamp, isPositive);
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Tweet executeOperation(GetTweetByIdOperation operation) {
        VoltTable[] results;
        try {
            results = client.callProcedure("GetTweetByIdProcedure",
                    operation.getTweetId()).getResults();

            VoltTable result = results[0];
            VoltTableRow row = result.fetchRow(0);

            return createTweetFromTweetRow(row);

        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByLanguageOperation operation) {
        VoltTable[] results;
        try {
            results = client.callProcedure("GetTweetByLanguageProcedure",
                    operation.getLanguage()).getResults();

            VoltTable result = results[0];
            int nRows = result.getRowCount();
            List<Tweet> tweetList = new ArrayList<>();
            for(int i=0; i<nRows; i++){
                tweetList.add(createTweetFromTweetRow(result.fetchRow(i)));
            }
            return tweetList;
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Tweet> executeOperation(GetTweetByTimestampIntervalOperation operation) {
        VoltTable[] results;
        try {
            results = client.callProcedure("GetTweetByTimestampIntervalProcedure",
                    operation.getStartTime(), operation.getEndTime()).getResults();
            VoltTable result = results[0];
            int nRows = result.getRowCount();
            List<Tweet> tweetList = new ArrayList<>();
            for(int i=0; i<nRows; i++){
                tweetList.add(createTweetFromTweetRow(result.fetchRow(i)));
            }
            return tweetList;
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Map<String, Integer> executeOperation(GetCountEngagementGroupByLanguageOperation operation) {
        VoltTable[] results;
        try {
            results = client.callProcedure("GetCountEngagementGroupByLanguageProcedure").getResults();
            VoltTable result = results[0];
            int nRows = result.getRowCount();
            Map<String, Integer> languageCountMap = new HashMap<>();
            for(int i=0; i<nRows; i++){
                VoltTableRow row = result.fetchRow(i);
                languageCountMap
                        .put((String) row.get("language", VoltType.STRING),
                                (int) row.get("c", VoltType.INTEGER));
            }
            return languageCountMap;
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args){
        final ClientConfig configuration = new ClientConfig();
        Client client = ClientFactory.createClient(configuration);
        VoltTable[] results;
        try {
            client.createConnection("localhost", 32777);

            results = client.callProcedure("GetCountEngagementGroupByLanguageProcedure").getResults();
            for (int i=0; i<results.length;i++) {
                VoltTable result = results[i];

                System.out.println(i);

                System.out.println(result.getRowCount());

                System.out.println(result.fetchRow(0).get("id", VoltType.STRING));
                System.out.println(result);
            }


        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
    }

    // This method creates a Tweet object from a row of the table 'tweet'
    private Tweet createTweetFromTweetRow(VoltTableRow row){
        return new Tweet(
                (String) row.get("id", VoltType.STRING),
                (int) row.get("creation_timestamp", VoltType.INTEGER),
                (String) row.get("language", VoltType.STRING)
        );
    }

    public static void handleResponse(ClientResponse cr) {
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            System.err.println(cr.getStatusString());
        }
    }


    @Override
    public Boolean executeOperation(BatchWriteOperation operation) {
        List<DataRow> dataRowList = operation.getDataRowList();

        // Get a BulkLoader for the table we want to load,
        // with a given batch size and one callback handles failures for any failed batches
        int batchSize = voltConfiguration.getBatchSize(); 
        boolean upsertMode = false;
        try {
            VoltBulkLoader bulkLoaderEngagement = client.getNewBulkLoader("engagement",
                    batchSize,
                    upsertMode,
                    new SessionBulkloaderFailureCallback(),
                    new SessionBulkloaderSuccessCallback());

            VoltBulkLoader bulkLoaderTweet = client.getNewBulkLoader("tweet",
                    batchSize,
                    upsertMode,
                    new SessionBulkloaderFailureCallback(),
                    new SessionBulkloaderSuccessCallback());

            VoltBulkLoader bulkLoaderUser = client.getNewBulkLoader("user",
                    batchSize,
                    upsertMode,
                    new SessionBulkloaderFailureCallback(),
                    new SessionBulkloaderSuccessCallback());

            for (int i=0; i<dataRowList.size(); i++) {

                Integer rowId = i;
                Integer rowIdUser1 = 2*i;
                Integer rowIdUser2 = rowIdUser1 + 1;

                // get current data row
                DataRow dataRow = dataRowList.get(i);

                // get data row params
                String creatorUserId = dataRow.getCreator().getUserId();
                int creatorVerified = dataRow.getCreator().getVerified() ? 1 : 0;
                String engagerUserId = dataRow.getEngager().getUserId();
                int engagerVerified = dataRow.getEngager().getVerified() ? 1 : 0;
                String tweetId = dataRow.getTweet().getTweetId();
                String language = dataRow.getTweet().getLanguage();
                long creationTimestamp = dataRow.getTweet().getCreationTimestamp();
                int isPositive = dataRow.getEngagement().getEngagement() ? 1 : 0;

                // create the rows for the various tables
                Object[] rowEngagement = {tweetId, engagerUserId, isPositive};
                Object[] rowTweet = {tweetId, language, creationTimestamp, creatorUserId};
                Object[] rowUser1 = {creatorUserId, creatorVerified};
                Object[] rowUser2 = {engagerUserId, engagerVerified};

                // load the rows for the various tables
                bulkLoaderEngagement.insertRow(rowId, rowEngagement);
                bulkLoaderTweet.insertRow(rowId, rowTweet);
                bulkLoaderUser.insertRow(rowIdUser1, rowUser1);
                bulkLoaderUser.insertRow(rowIdUser2, rowUser2);

            }

            bulkLoaderEngagement.drain();
            bulkLoaderTweet.drain();
            bulkLoaderUser.drain();

            client.drain();

            bulkLoaderEngagement.close();
            bulkLoaderTweet.close();
            bulkLoaderUser.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }


    // Implement a BulkLoaderFailureCallback for your BulkLoader
    public static class SessionBulkloaderFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse cr) {
            handleResponse(cr);
        }
    }

    // Implement a BulkLoaderSuccessCallback for your BulkLoader
    public static class SessionBulkloaderSuccessCallback implements BulkLoaderSuccessCallback {
        @Override
        public void success(Object rowHandle, ClientResponse cr) {
            handleResponse(cr);
        }
    }
}
