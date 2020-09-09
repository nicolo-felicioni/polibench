package voltdb;

import org.voltdb.client.ClientConfig;

public class VoltDBConfiguration {
    private ClientConfig configuration;
    private String address;
    private Integer port;
    private boolean multipleInsertsInSingleProcedure = false;
    private int batchSize = 1;

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }


    public ClientConfig getConfiguration() {
        return configuration;
    }

    public boolean isMultipleInsertsInSingleProcedure() {
        return multipleInsertsInSingleProcedure;
    }

    public String getAddress() {
        return address;
    }

    public Integer getPort() {
        return port;
    }

    public VoltDBConfiguration(String address, Integer port){
        this.configuration = new ClientConfig();
        this.address = address;
        this.port = port;
    }


    public VoltDBConfiguration(String address, Integer port, boolean multipleInsertsInSingleProcedure){
        this.configuration = new ClientConfig();
        this.address = address;
        this.port = port;
        this.multipleInsertsInSingleProcedure = multipleInsertsInSingleProcedure;
    }
}
