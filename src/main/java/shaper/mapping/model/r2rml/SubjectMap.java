package shaper.mapping.model.r2rml;

import java.net.URI;
import java.util.Set;

public class SubjectMap extends TermMap {
    private Set<URI> classIRIs; // the size of classIRIs could be zero.

    SubjectMap(Set<URI> classIRIs) { this.classIRIs = classIRIs; }

    public Set<URI> getClassIRIs() { return classIRIs; }
}
