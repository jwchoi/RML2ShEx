package shaper.mapping.model.r2rml;

import java.net.URI;
import java.util.Set;

public class SubjectMap extends TermMap {
    private Set<URI> classes; // the size of classIRIs could be zero.

    SubjectMap(Set<URI> classes) { this.classes = classes; }

    public Set<URI> getClasses() { return classes; }
}
