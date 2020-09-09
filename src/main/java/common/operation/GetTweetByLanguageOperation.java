package common.operation;

import common.Database;
import common.Operation;
import common.data.DataRow;
import common.data.entity.Tweet;

import java.util.List;

public class GetTweetByLanguageOperation implements Operation<List<Tweet>> {

    private String language;

    public GetTweetByLanguageOperation(String language) {
        this.language = language;
    }

    public String getLanguage() {
        return language;
    }

    @Override
    public List<Tweet> visit(Database db) {
        return db.executeOperation(this);
    }
}
