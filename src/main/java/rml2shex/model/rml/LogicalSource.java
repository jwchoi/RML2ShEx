package rml2shex.model.rml;

import java.net.URI;
import java.util.Optional;

public class LogicalSource extends LogicalTable {
    private Optional<String> query; // rml:query overrides rr:sqlQuery

    private Source source; // rml:source
    private Optional<URI> referenceFormulation;
    private Optional<String> iterator;

    LogicalSource() {
        query = Optional.empty();

        referenceFormulation = Optional.empty();
        iterator = Optional.empty();
    }

    public Source getSource() { return source; }
    void setSource(Source source) { this.source = source; }

    public URI getReferenceFormulation() { return referenceFormulation.orElse(null); }

    void setReferenceFormulation(URI referenceFormulation) {
        this.referenceFormulation = Optional.ofNullable(referenceFormulation);
    }

    public String getIterator() { return iterator.orElse(null); }
    void setIterator(String iterator) { this.iterator = Optional.ofNullable(iterator); }

    void setQuery(String query) { this.query = Optional.ofNullable(query); }
    public String getQuery() { return query.orElse(null); }
}
