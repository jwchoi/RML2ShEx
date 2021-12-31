package shaper.mapping.model.rml;

import janus.database.DBMSTypes;
import shaper.mapping.Symbols;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class LogicalSource {
    private Optional<URI> uri;

    private Optional<String> tableName; // rr:tableName
    private Optional<String> sqlQuery; // rr:sqlQuery
    private Set<URI> sqlVersions; // rr:sqlVersion, which is an IRI

    private Source source; // rml:source

    LogicalSource() {
        uri = Optional.empty();

        tableName = Optional.empty();
        sqlQuery = Optional.empty();
        sqlVersions = new TreeSet<>();
    }

    void setSource(Source source) { this.source = source; }

    public void setUri(URI uri) {
        this.uri = Optional.ofNullable(uri);
    }

    public void setTableName(String tableName) { this.tableName = Optional.ofNullable(tableName); }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = Optional.ofNullable(sqlQuery);
    }

    public void addSqlVersion(URI sqlVersion) {
        sqlVersions.add(sqlVersion);
    }

    public String getSqlQuery(DBMSTypes DBMSType) {
        if (sqlQuery.isPresent())
            return sqlQuery.get();

        if (tableName.isPresent()) {
            String tableName = this.tableName.get();
            if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
                switch (DBMSType) {
                    case MARIADB:
                    case MYSQL:
                        tableName = Symbols.GRAVE_ACCENT + tableName.substring(1, tableName.length() - 1) + Symbols.GRAVE_ACCENT;
                        break;
                }
            }

            return "SELECT * FROM " + tableName;
        }

        return null;
    }

    public Optional<URI> getUri() { return uri; }
}
