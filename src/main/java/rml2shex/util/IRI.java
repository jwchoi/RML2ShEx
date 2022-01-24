package rml2shex.util;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class IRI implements Comparable<IRI> {
    private URI iri;

    private String prefixLabel;
    private URI prefixIRI;
    private String localPart;

    public static IRI createIRI(URI uri, Map<String, String> prefixMap) {
        if (uri == null) return null;

        Set<String> prefixes = prefixMap.keySet();

        Optional<String> prefixLabel = prefixes.stream()
                .filter(prefix -> uri.toASCIIString().startsWith(prefixMap.get(prefix)))
                .max((prefix1, prefix2) -> prefixMap.get(prefix1).compareTo(prefixMap.get(prefix2)));

        if (prefixLabel.isPresent()) {
            URI prefixIRI = URI.create(prefixMap.get(prefixLabel.get()));
            String localPart = uri.toASCIIString().replace(prefixIRI.toASCIIString(), Symbols.EMPTY);

            if (isLocalPartValid(localPart)) return new IRI(prefixLabel.get(), prefixIRI, localPart);
        }

        return new IRI(uri);
    }

    // temporary implementation
    private static boolean isLocalPartValid(String localPart) {
        return localPart.indexOf(Symbols.SLASH) == -1;
    }

    private IRI(URI uri) { iri = uri; }

    // https://www.w3.org/TR/turtle/#sec-grammar-grammar
    // https://www.w3.org/TR/turtle/#sec-iri
    public IRI(String prefixLabel, URI prefixIRI, String localPart) {
        this(URI.create(prefixIRI + localPart));

        this.prefixLabel = prefixLabel;
        this.prefixIRI = prefixIRI;
        this.localPart = localPart;
    }

    public String getPrefixLabel() { return prefixLabel; }
    public URI getPrefixIRI() { return prefixIRI; }

    public String getPrefixedName() { return prefixLabel != null ? prefixLabel + Symbols.COLON + localPart : null; }
    public String getAbsoluteIRI() { return "<" + iri.toASCIIString() + ">"; }
    public String getLocalPart() { return localPart; }

    public String getPrefixedNameOrElseAbsoluteIRI() {
        String prefixedName = getPrefixedName();
        return prefixedName != null ? prefixedName : getAbsoluteIRI();
    }

    @Override
    public int compareTo(IRI o) {
        return getAbsoluteIRI().compareTo(o.getAbsoluteIRI());
    }

    @Override
    public String toString() {
        return iri.toASCIIString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IRI)) return false;
        IRI id = (IRI) o;
        return Objects.equals(prefixLabel, id.prefixLabel) && Objects.equals(prefixIRI, id.prefixIRI) && Objects.equals(getLocalPart(), id.getLocalPart());
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefixLabel, prefixIRI, getLocalPart());
    }
}
