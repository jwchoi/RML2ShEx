package shaper.mapping.model.rml;

import java.net.URI;
import java.util.Optional;

public class ObjectMap extends TermMap {
    private Optional<String> language; // rr:language
    private Optional<URI> datatype; // rr:datatype

    ObjectMap() {
        language = Optional.empty();
        datatype = Optional.empty();
    }

    void setLanguage(String language) {
        if (language != null) {
            this.language = Optional.of(language);
            setTermType(TermTypes.LITERAL);
        }
    }

    void setDatatype(URI datatype) {
        if (datatype != null) {
            this.datatype = Optional.of(datatype);
            setTermType(TermTypes.LITERAL);
        }
    }

    public Optional<String> getLanguage() {
        return language;
    }

    public Optional<URI> getDatatype() {
        return datatype;
    }

    @Override
    void setColumn(String column) {
        if (column != null) {
            super.setColumn(column);
            setTermType(TermTypes.LITERAL);
        }
    }
}
