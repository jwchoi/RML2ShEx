package rml2shex.model.rml;

import rml2shex.commons.IRI;

import java.util.Set;

public class SubjectMap extends TermMap {
    private Set<IRI> classes; // the size of classIRIs could be zero.
    private Set<GraphMap> graphMaps; // the size of graphMaps >= 0

    void setClasses(Set<IRI> classes) { this.classes = classes; }

    public Set<IRI> getClasses() { return classes; }

    void setGraphMaps(Set<GraphMap> graphMaps) { this.graphMaps = graphMaps; }
}
