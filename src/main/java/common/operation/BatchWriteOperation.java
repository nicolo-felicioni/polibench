package common.operation;

import common.Database;
import common.Operation;
import common.data.DataRow;

import java.util.List;

public class BatchWriteOperation implements Operation<Boolean> {

    private List<DataRow> dataRowList;

    public BatchWriteOperation(List<DataRow> dataRowList) {
        this.dataRowList = dataRowList;
    }

    public List<DataRow> getDataRowList() {
        return dataRowList;
    }

    @Override
    public Boolean visit(Database db) {
        return db.executeOperation(this);
    }
}
