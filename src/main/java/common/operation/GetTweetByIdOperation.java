package common.operation;

import common.Database;
import common.Operation;
import common.data.DataRow;
import common.data.entity.Tweet;

import java.util.List;

public class GetTweetByIdOperation implements Operation<Tweet> {

    private String tweetId;

    public GetTweetByIdOperation(String tweetId) {
        this.tweetId = tweetId;
    }

    public String getTweetId() {
        return tweetId;
    }

    @Override
    public Tweet visit(Database db) {
        return db.executeOperation(this);
    }
}
