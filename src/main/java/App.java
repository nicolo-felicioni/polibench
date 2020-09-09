import cassandra.CassandraConfiguration;
import cassandra.CassandraDatabase;
import cassandra.NewCassandraDatabase;
import com.google.gson.Gson;
import common.Benchmark;
import common.Database;
import common.data.Data;
import mongodb.MongoConfiguration;
import mongodb.MongoDatabase;
import mysql.MySQLConfiguration;
import mysql.MySQLDatabase;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.voltdb.client.*;
import voltdb.VoltDB;
import voltdb.VoltDBConfiguration;

public class App {

    public static void main(String[] args) throws IOException {

        String input_filename = args[0];
        String output_filename = args[1];

        // Dataset to be used
        Data data = new Data();
        data.readFromFile(input_filename);

        // Databases to be benched
        List<Database> databases = new ArrayList<>();
        // Adding Vitess database
//        final MySQLConfiguration vitessConfiguration = new MySQLConfiguration("127.0.0.1", 15306, "benchmark_vitess", "root", "password");
//        MySQLDatabase vitessDatabase = new MySQLDatabase(vitessConfiguration);
//        databases.add(vitessDatabase);
//        // Adding MySQL database
//        final MySQLConfiguration mySQLConfiguration = new MySQLConfiguration("127.0.0.1", 3306, "benchmark_mysql", "root", "password");
//        MySQLDatabase mySQLDatabase = new MySQLDatabase(mySQLConfiguration);
//        databases.add(mySQLDatabase);
//        // Adding MongoDB database
//        final MongoConfiguration mongoConfiguration = new MongoConfiguration("127.0.0.1", 27017, "benchmark", Boolean.FALSE, "user", "password", "benchmark_collection");
//        MongoDatabase mongoDatabase = new MongoDatabase(mongoConfiguration);
//        databases.add(mongoDatabase);
//        // Adding VoltDB
//        final VoltDBConfiguration voltConfiguration = new VoltDBConfiguration("127.0.0.1", 32777);
//        VoltDB voltDB = new VoltDB(voltConfiguration);
//        databases.add(voltDB);
        // Adding Cassandra
        final CassandraConfiguration cassandraConfiguration = new CassandraConfiguration("151.0.231.141", 9042, "usde", 3, "password");
        NewCassandraDatabase cassandraDatabase = new NewCassandraDatabase(cassandraConfiguration);
        databases.add(cassandraDatabase);


        // Initialize the benchmark
        Benchmark benchmark = new Benchmark(databases, data);

        // Run the benchmark
        Map<Database, Map<String, SummaryStatistics>> result = benchmark.runBenchmark();

        // Sage the results
        BufferedWriter writer = Files.newBufferedWriter(Paths.get(output_filename));
        databases.forEach(db -> {
            Map<String, SummaryStatistics> stats = result.get(db);
            stats.keySet().forEach(k -> {
                SummaryStatistics v = stats.get(k);
                double min = v.getMin();
                double mean = v.getMean();
                double max = v.getMax();
                double sum = v.getSum();
                double var = v.getVariance();
                String row = db.getClass().getSimpleName().toLowerCase() + "," + k + "," + min + "," + mean + "," + max + "," + sum + "," + var + '\n';
                System.out.println(row);
                try {
                    writer.write(row);
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        });
    }

}
