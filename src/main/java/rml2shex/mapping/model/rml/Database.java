package rml2shex.mapping.model.rml;

import java.net.URI;

public class Database {
    private URI uri;

    private String jdbcDSN;
    private String jdbcDriver;
    private String username;
    private String password;

    Database(URI uri, String jdbcDSN, String jdbcDriver, String username, String password) {
        this.uri = uri;

        this.jdbcDSN = jdbcDSN;
        this.jdbcDriver = jdbcDriver;
        this.username = username;
        this.password = password;
    }
}
