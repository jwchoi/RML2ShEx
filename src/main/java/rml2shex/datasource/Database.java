package rml2shex.datasource;

import java.net.URI;
import java.util.Optional;

public class Database {
    private Optional<URI> uri;

    private String jdbcDSN;
    private String jdbcDriver;
    private String username;
    private String password;

    public Database(String jdbcDSN, String jdbcDriver, String username, String password) {
        this(null, jdbcDSN, jdbcDriver, username, password);
    }

    public Database(URI uri, String jdbcDSN, String jdbcDriver, String username, String password) {
        this.uri = Optional.ofNullable(uri);

        this.jdbcDSN = jdbcDSN;
        this.jdbcDriver = jdbcDriver;
        this.username = username;
        this.password = password;
    }

    public URI getUri() { return uri.orElse(null); }

    public String getJdbcDSN() { return jdbcDSN; }

    public String getJdbcDriver() { return jdbcDriver; }

    public String getUsername() { return username; }

    public String getPassword() { return password; }
}
