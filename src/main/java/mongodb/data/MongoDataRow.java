package mongodb.data;

import common.data.DataRow;
import common.data.entity.Engagement;
import common.data.entity.Tweet;
import common.data.entity.User;
import org.bson.types.ObjectId;

public class MongoDataRow extends DataRow {

    private ObjectId _id;

    public MongoDataRow() {
    }

    public MongoDataRow(DataRow dataRow) {
        super(dataRow.getTweet(), dataRow.getEngager(), dataRow.getCreator(), dataRow.getEngagement());
    }

    public ObjectId get_id() {
        return _id;
    }

    public void set_id(ObjectId _id) {
        this._id = _id;
    }
}
