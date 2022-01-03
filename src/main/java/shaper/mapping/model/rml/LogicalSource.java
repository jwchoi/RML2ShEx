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
    private Optional<String> query; // rml:query
    private Set<URI> sqlVersions; // rr:sqlVersion, which is an IRI

    private Source source; // rml:source
    private Optional<URI> referenceFormulation;
    private Optional<String> iterator;

    LogicalSource() {
        uri = Optional.empty();

        tableName = Optional.empty();
        query = Optional.empty();
        sqlVersions = new TreeSet<>();

        referenceFormulation = Optional.empty();
        iterator = Optional.empty();
    }

    void setSource(Source source) { this.source = source; }

    void setReferenceFormulation(URI referenceFormulation) {
        this.referenceFormulation = Optional.ofNullable(referenceFormulation);
    }

    void setIterator(String iterator) { this.iterator = Optional.ofNullable(iterator); }

    public void setUri(URI uri) {
        this.uri = Optional.ofNullable(uri);
    }

    void setTableName(String tableName) { this.tableName = Optional.ofNullable(tableName); }

    public void setQuery(String query) { this.query = Optional.ofNullable(query); }

    void setSqlVersions(Set<URI> sqlVersions) { this.sqlVersions = sqlVersions; }

    public String getQuery(DBMSTypes DBMSType) {
        if (query.isPresent())
            return query.get();

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
