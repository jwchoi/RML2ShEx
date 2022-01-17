package rml2shex.util;

import java.net.URI;
import java.util.Objects;

public class Id implements Comparable<Id> {
    private String prefixLabel;
    private URI prefixIRI;
    private String localPart;

    // https://www.w3.org/TR/turtle/#sec-grammar-grammar
    // https://www.w3.org/TR/turtle/#sec-iri
    public Id(String prefixLabel, URI prefixIRI, String localPart) {
        this.prefixLabel = prefixLabel;
        this.prefixIRI = prefixIRI;
        this.localPart = localPart;
    }

    public String getPrefixedName() { return prefixLabel + ":" + localPart; }
    public String getAbsoluteIRI() { return "<" + URI.create(prefixIRI + localPart).toASCIIString() + ">"; }
    public String getLocalPart() { return localPart; }

    @Override
    public int compareTo(Id o) {
        return getAbsoluteIRI().compareTo(o.getAbsoluteIRI());
    }

    @Override
    public String toString() {
        return URI.create(prefixIRI + localPart).toASCIIString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Id)) return false;
        Id id = (Id) o;
        return Objects.equals(prefixLabel, id.prefixLabel) && Objects.equals(prefixIRI, id.prefixIRI) && Objects.equals(getLocalPart(), id.getLocalPart());
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefixLabel, prefixIRI, getLocalPart());
    }
}
