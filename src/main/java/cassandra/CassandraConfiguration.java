package cassandra;

import common.Configuration;

public class CassandraConfiguration extends Configuration {

    private final String address;
    private final Integer port;
    private final String keyspace;
    private final Integer replicationFactor;
    private final String replicationStrategy;

    public CassandraConfiguration(String address, Integer port, String keyspace, Integer replicationFactor, String replicationStrategy) {
        this.address = address;
        this.port = port;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
        this.replicationStrategy = replicationStrategy;
    }

    public String getAddress() {
        return address;
    }

    public Integer getPort() {
        return port;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public String getReplicationStrategy() {
        return replicationStrategy;
    }
}
