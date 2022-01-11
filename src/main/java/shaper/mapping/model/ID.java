package shaper.mapping.model;

import java.net.URI;

public class ID implements Comparable<ID> {
    private URI id;

    private String prefixLabel;
    private URI prefixIRI;
    private String localPart;

    // https://www.w3.org/TR/turtle/#sec-grammar-grammar
    // https://www.w3.org/TR/turtle/#sec-iri
    public ID(String prefixLabel, URI prefixIRI, String localPart) {
        this.prefixLabel = prefixLabel;
        this.prefixIRI = prefixIRI;
        this.localPart = localPart;

        id = URI.create(prefixIRI + localPart);
    }

    public String getPrefixedName() { return prefixLabel + ":" + localPart; }
    public String getAbsoluteIRI() { return "<" + id.toASCIIString() + ">"; }
    public String getRelativeIRI() { return "<" + localPart + ">"; }

    public String getLocalPart() { return localPart; }


    @Override
    public int compareTo(ID o) {
        return getAbsoluteIRI().compareTo(o.getAbsoluteIRI());
    }

    @Override
    public String toString() {
        return id.toASCIIString();
    }
}
