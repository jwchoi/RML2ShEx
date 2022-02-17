package rml2shex.datasource;

import rml2shex.commons.IRI;

import java.net.URI;
import java.util.Optional;

public class Column {
    private String name;
    private Optional<String> type; // acquired from the data source

    private Optional<String> minValue; // acquired from the data source
    private Optional<String> maxValue; // acquired from the data source

    private Optional<Integer> minLength; // acquired from the data source
    private Optional<Integer> maxLength; // acquired from the data source

    public Column(String name) {
        this.name = name;

        minValue = Optional.empty();
        maxValue = Optional.empty();

        minLength = Optional.empty();
        maxLength = Optional.empty();
    }

    public String getName() { return name; }
    public String getNameInBackticks() {
        return (name.startsWith("`") && name.endsWith("`")) ? getName() : "`" + name + "`";
    }

    void setType(String type) {if (type != null) this.type = Optional.of(type); }

    public Optional<String> getMinValue() { return minValue; }
    void setMinValue(String minValue) { if (minValue != null) this.minValue = Optional.of(minValue); }

    public Optional<String> getMaxValue() { return maxValue; }
    void setMaxValue(String maxValue) { if (maxValue != null) this.maxValue = Optional.of(maxValue); }

    public Optional<Integer> getMinLength() { return minLength; }
    void setMinLength(String minLength) { this.minLength = Optional.of(Integer.parseUnsignedInt(minLength)); }

    public Optional<Integer> getMaxLength() { return maxLength; }
    void setMaxLength(String maxLength) { this.maxLength = Optional.of(Integer.parseUnsignedInt(maxLength)); }

    public Optional<IRI> getRdfDatatype() {
        Optional<IRI> rdfDatatype = Optional.empty();

        if (type.isEmpty()) return rdfDatatype;

        String prefixLabel = "xsd";
        URI prefixIRI = URI.create("http://www.w3.org/2001/XMLSchema#");
        String localPart = null;

        switch (type.get()) {
            case "BINARY": localPart = "hexBinary"; break;
            case "DECIMAL": case "DEC": case "NUMERIC": localPart = "decimal"; break;
            case "SHORT": case "SMALLINT": case "INT": case "INTEGER": case "LONG": case "BIGINT": localPart = "integer"; break;
            case "FLOAT": case "REAL": case "DOUBLE": localPart = "double"; break;
            case "BOOLEAN": localPart = "boolean"; break;
            case "DATE": localPart = "date"; break;
            case "TIMESTAMP": localPart = "dateTime"; break;
            default: break;
        }

        if (localPart != null) rdfDatatype = Optional.of(new IRI(prefixLabel, prefixIRI, localPart));

        return rdfDatatype;
    }
}
