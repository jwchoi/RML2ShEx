package rml2shex.model.rml;

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

    public String getTableName() { return tableName.orElse(null); }
    void setTableName(String tableName) { this.tableName = Optional.ofNullable(tableName); }

    public String getSqlQuery() { return sqlQuery.orElse(null); }
    public void setSqlQuery(String sqlQuery) { this.sqlQuery = Optional.ofNullable(sqlQuery); }

    void setSqlVersions(Set<URI> sqlVersions) { this.sqlVersions = sqlVersions; }
    public Set<URI> getSqlVersions() { return sqlVersions; }

    public URI getUri() { return uri; }
}
