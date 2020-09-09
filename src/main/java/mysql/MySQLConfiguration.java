package mysql;

import common.Configuration;

public class MySQLConfiguration extends Configuration {

    private final String address;
    private final Integer port;
    private final String dbName;
    private final String username;
    private final String password;

    public MySQLConfiguration(String address, Integer port, String dbName, String username, String password) {
        this.address = address;
        this.port = port;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
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

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

}
