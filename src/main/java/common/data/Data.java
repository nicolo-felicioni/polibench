package common.data;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class Data {

    private final List<DataRow> dataRowList;

    public Data() {
        this.dataRowList = new ArrayList<DataRow>();
    }

    public static void main(String[] args) throws IOException {
        Data d = new Data();
        d.readFromFile("train_days_1.csv.gz");
    }

    public void readFromFile(String filename) throws IOException {
        InputStream fileStream = new FileInputStream(filename);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(decoder);
        this.dataRowList.addAll(reader.lines().map(DataRow::fromText).collect(Collectors.toList()));
    }

    public Stream<DataRow> getRowStream() {
        return dataRowList.stream();
    }

}

