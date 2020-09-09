package voltdb.procedures;

import common.data.DataRow;
import common.data.entity.Engagement;
import common.data.entity.Tweet;
import common.data.entity.User;
import org.voltdb.*;

public class SingleWriteProcedure extends VoltProcedure {

    private final SQLStmt writeUserStmt = new SQLStmt(
            "INSERT INTO user " +
                    "VALUES (?, ?);");

    private final SQLStmt writeTweetStmt = new SQLStmt(
            "INSERT INTO tweet " +
                    "VALUES (?, ?, ?, ?);");

    private final SQLStmt writeEngagementStmt = new SQLStmt(
            "INSERT INTO engagement " +
                    "VALUES (?, ?, ?);");


    public long run(String creatorUserId, int creatorVerified, String engagerUserId, int engagerVerified,
                    String tweetId, String language, long creationTimestamp, int isPositive) throws VoltAbortException {

        voltQueueSQL(writeUserStmt, creatorUserId, creatorVerified);
        voltQueueSQL(writeUserStmt, engagerUserId, engagerVerified);
        voltQueueSQL(writeTweetStmt, tweetId, language, creationTimestamp, creatorUserId);
        voltQueueSQL(writeEngagementStmt, tweetId, engagerUserId, isPositive);

        voltExecuteSQL();
        return 0;
    }
}
