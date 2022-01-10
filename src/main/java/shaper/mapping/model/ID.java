package shaper.mapping.model;

import java.net.URI;

public class ID {
    private String prefixLabel;
    private URI prefixIRI;
    private String localPart;

    // https://www.w3.org/TR/turtle/#sec-grammar-grammar
    // https://www.w3.org/TR/turtle/#sec-iri
    public ID(String prefixLabel, URI prefixIRI, String localPart) {
        this.prefixLabel = prefixLabel;
        this.prefixIRI = prefixIRI;
        this.localPart = localPart;
    }

    public String getPrefixedName() { return prefixLabel + ":" + localPart; }
    public String getAbsoluteIRI() { return "<" + URI.create(prefixIRI + localPart).toASCIIString() + ">"; }
    public String getRelativeIRI() { return "<" + localPart + ">"; }
}
