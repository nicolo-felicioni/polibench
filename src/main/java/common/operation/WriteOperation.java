package common.operation;

import common.Database;
import common.Operation;
import common.data.DataRow;

public class WriteOperation implements Operation<Boolean> {

    private DataRow dataRow;

    public WriteOperation(DataRow dataRow) {
        this.dataRow = dataRow;
    }

    public DataRow getDataRow() {
        return dataRow;
    }

    @Override
    public Boolean visit(Database db) {
        return db.executeOperation(this);
    }
}
