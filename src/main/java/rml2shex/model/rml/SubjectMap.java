package rml2shex.model.rml;

import rml2shex.commons.IRI;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

public class SubjectMap extends TermMap {
    private Set<IRI> classes; // the size of classIRIs could be zero.
    private Set<GraphMap> graphMaps; // the size of graphMaps >= 0

    void setClasses(Set<IRI> classes) { this.classes = classes; }

    public Set<IRI> getClasses() { return classes; }

    void setGraphMaps(Set<GraphMap> graphMaps) { this.graphMaps = graphMaps; }

    @Override
    void setTermType(URI uri) throws Exception {
        if (uri != null && uri.equals(TermTypes.LITERAL.getUri())) {
            throw new Exception("The presence of rr:termType rr:Literal on rr:subjectMap, which is invalid.");
        }

        super.setTermType(uri);
    }
}
