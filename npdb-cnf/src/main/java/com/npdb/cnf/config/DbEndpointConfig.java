package com.npdb.cnf.config;

import java.util.Objects;

/**
 * One Postgres endpoint addressed by Multus static IP.
 */
public final class DbEndpointConfig {

    private final String id;
    private final String host;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final DbRole role;

    public DbEndpointConfig(String id, String host, int port, String database,
                            String username, String password, DbRole role) {
        this.id = Objects.requireNonNull(id, "id");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.database = Objects.requireNonNull(database, "database");
        this.username = Objects.requireNonNull(username, "username");
        this.password = Objects.requireNonNull(password, "password");
        this.role = Objects.requireNonNull(role, "role");
    }

    public String id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String database() {
        return database;
    }

    public String username() {
        return username;
    }

    public String password() {
        return password;
    }

    public DbRole role() {
        return role;
    }
}
