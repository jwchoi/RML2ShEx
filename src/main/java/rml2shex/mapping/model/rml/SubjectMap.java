package rml2shex.mapping.model.rml;

import java.net.URI;
import java.util.Set;

public class SubjectMap extends TermMap {
    private Set<URI> classes; // the size of classIRIs could be zero.
    private Set<GraphMap> graphMaps; // the size of graphMaps >= 0

    void setClasses(Set<URI> classes) { this.classes = classes; }

    public Set<URI> getClasses() { return classes; }

    void setGraphMaps(Set<GraphMap> graphMaps) { this.graphMaps = graphMaps; }
}
