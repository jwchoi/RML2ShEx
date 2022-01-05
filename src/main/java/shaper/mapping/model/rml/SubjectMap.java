package shaper.mapping.model.rml;

import java.net.URI;
import java.util.Set;

public class SubjectMap extends TermMap {
    private Set<URI> classIRIs; // the size of classIRIs could be zero.
    private Set<GraphMap> graphMaps; // the size of graphMaps >= 0

    void setClassIRIs(Set<URI> classIRIs) { this.classIRIs = classIRIs; }

    public Set<URI> getClassIRIs() { return classIRIs; }

    void setGraphMaps(Set<GraphMap> graphMaps) { this.graphMaps = graphMaps; }
}
