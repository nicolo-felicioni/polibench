package common.operation;

import common.Database;
import common.Operation;

import java.util.List;
import java.util.Map;

public class GetCountEngagementGroupByLanguageOperation implements Operation<Map<String, Integer>> {

    @Override
    public Map<String, Integer> visit(Database db) {
        return db.executeOperation(this);
    }

}
