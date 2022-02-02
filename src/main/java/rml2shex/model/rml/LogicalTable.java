package rml2shex.model.rml;

import rml2shex.datasource.db.DBMSTypes;
import rml2shex.commons.Symbols;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class LogicalTable {
    private URI uri;

    private Optional<String> tableName; // rr:tableName
    private Optional<String> sqlQuery; // rr:sqlQuery
    private Set<URI> sqlVersions; // rr:sqlVersion, which is an IRI

    LogicalTable() {
        tableName = Optional.empty();
        sqlQuery = Optional.empty();
        sqlVersions = new TreeSet<>();
    }

    void setUri(URI uri) { this.uri = uri; }

    void setTableName(String tableName) { this.tableName = Optional.ofNullable(tableName); }

    public void setSqlQuery(String sqlQuery) { this.sqlQuery = Optional.ofNullable(sqlQuery); }

    void setSqlVersions(Set<URI> sqlVersions) { this.sqlVersions = sqlVersions; }

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

    public URI getUri() { return uri; }
}
