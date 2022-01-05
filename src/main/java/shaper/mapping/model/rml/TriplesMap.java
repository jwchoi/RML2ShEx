package shaper.mapping.model.rml;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TriplesMap {

    private URI uri;
    private Optional<LogicalTable> logicalTable;
    private Optional<LogicalSource> logicalSource;
    private SubjectMap subjectMap;
    private List<PredicateObjectMap> predicateObjectMaps;

    TriplesMap(URI uri) {
        this.uri = uri;
        predicateObjectMaps = new ArrayList<>();
    }

    TriplesMap(URI uri, LogicalTable logicalTable, SubjectMap subjectMap) {
        this(uri);

        this.logicalTable = Optional.of(logicalTable);
        this.subjectMap = subjectMap;
        predicateObjectMaps = new ArrayList<>();
    }

    void setLogicalSource(LogicalSource logicalSource) { this.logicalSource = Optional.of(logicalSource); }

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
