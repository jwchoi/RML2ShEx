package rml2shex.util;

import java.net.URI;

public enum XSDs {
    XSD_ANY_TYPE(URI.create(PrefixMap.getURI("xsd") + "anyType")),
    XSD_ANY_URI(URI.create(PrefixMap.getURI("xsd") + "anyURI")),
    XSD_BASE_64_BINARY(URI.create(PrefixMap.getURI("xsd") + "base64Binary")),
    XSD_BOOLEAN(URI.create(PrefixMap.getURI("xsd") + "boolean")),
    XSD_DATE(URI.create(PrefixMap.getURI("xsd") + "date")),
    XSD_DATE_TIME(URI.create(PrefixMap.getURI("xsd") + "dateTime")),
    XSD_DECIMAL(URI.create(PrefixMap.getURI("xsd") + "decimal")),
    XSD_DOUBLE(URI.create(PrefixMap.getURI("xsd") + "double")),
    XSD_HEX_BINARY(URI.create(PrefixMap.getURI("xsd") + "hexBinary")),
    XSD_INTEGER(URI.create(PrefixMap.getURI("xsd") + "integer")),
    XSD_STRING(URI.create(PrefixMap.getURI("xsd") + "string")),
    XSD_TIME(URI.create(PrefixMap.getURI("xsd") + "time"));

    private URI uri;

    private XSDs(URI uri) { this.uri = uri; }

    public String getRelativeIRI() {
        String xsd = PrefixMap.getURI("xsd").toString();
        return uri.toString().replace(xsd, "xsd" + Symbols.COLON);
    }

    public URI getURI() { return uri; }

    @Override
    public String toString() { return uri.toString(); }
}
