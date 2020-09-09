
package common.operation;

import common.Database;
import common.Operation;
import common.data.DataRow;
import common.data.entity.Tweet;

import java.util.List;

public class GetTweetByTimestampIntervalOperation implements Operation<List<Tweet>> {

    private Integer startTime;
    private Integer endTime;

    public GetTweetByTimestampIntervalOperation(Integer startTime, Integer endTime) {
        if (startTime < endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        } else {
            // TODO: should I throw an exception?
            System.out.println("start time {" + startTime + "} should be lower than end time {" + endTime +"}.");
        }
    }

    public Integer getStartTime() {
        return startTime;
    }

    public Integer getEndTime() {
        return endTime;
    }

    @Override
    public List<Tweet> visit(Database db) {
        return db.executeOperation(this);
    }
}
