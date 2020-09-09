package mongodb;

import common.Configuration;

import java.io.IOException;

public class MongoConfiguration extends Configuration {

    private final String address;
    private final Integer port;
    private final String dbName;
    private final Boolean useAuthentication;
    private final String username;
    private final String password;
    private final String collectionName;

    public MongoConfiguration(String address, Integer port, String dbName, Boolean useAuthentication, String username, String password, String collectionName) {
        this.address = address;
        this.port = port;
        this.dbName = dbName;
        this.useAuthentication = useAuthentication;
        this.username = username;
        this.password = password;
        this.collectionName = collectionName;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }

    public Boolean getUseAuthentication() {
        return useAuthentication;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getCollectionName() {
        return collectionName;
    }

}
