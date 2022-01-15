package rml2shex.mapping.model.rml;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TriplesMap {

    private URI uri;
    private Optional<LogicalTable> logicalTable; // It can be a logicalSource which is a subclass of LogicalTable.
    private SubjectMap subjectMap;
    private List<PredicateObjectMap> predicateObjectMaps;

    TriplesMap(URI uri) {
        this.uri = uri;

        predicateObjectMaps = new ArrayList<>();
    }

    void setLogicalSource(LogicalSource logicalSource) { this.logicalTable = Optional.of(logicalSource); }

    void setLogicalTable(LogicalTable logicalTable) { this.logicalTable = Optional.of(logicalTable); }

    void setSubjectMap(SubjectMap subjectMap) { this.subjectMap = subjectMap; }

    public void addPredicateObjectMap(PredicateObjectMap predicateObjectMap) {
        predicateObjectMaps.add(predicateObjectMap);
    }

    public LogicalTable getLogicalTable() { return logicalTable.orElse(null); }

    public SubjectMap getSubjectMap() { return subjectMap; }

    public URI getUri() { return uri; }

    public List<PredicateObjectMap> getPredicateObjectMaps() { return predicateObjectMaps; }

    @Override
    public int hashCode() {
        return Objects.hash(uri);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TriplesMap)
            return uri.equals(((TriplesMap) obj).uri);

        return super.equals(obj);
    }
}
