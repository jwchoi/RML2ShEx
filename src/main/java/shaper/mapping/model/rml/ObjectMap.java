package shaper.mapping.model.rml;

import java.net.URI;
import java.util.Optional;

public class ObjectMap extends TermMap {
    private Optional<URI> datatype; // rr:datatype

    private Optional<LanguageMap> languageMap; // rml:languageMap -> the shortest form is rr:language

    ObjectMap() {
        languageMap = Optional.empty();
        datatype = Optional.empty();
    }

    void setLanguageMap(LanguageMap languageMap) {
        if (languageMap != null) {
            this.languageMap = Optional.of(languageMap);
            setTermType(TermTypes.LITERAL);
        }
    }

    void setDatatype(URI datatype) {
        if (datatype != null) {
            this.datatype = Optional.of(datatype);
            setTermType(TermTypes.LITERAL);
        }
    }

    public Optional<LanguageMap> getLanguageMap() {
        return languageMap;
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

    @Override
    void setReference(String reference) {
        if (reference != null) {
            super.setColumn(reference);
            setTermType(TermTypes.LITERAL);
        }
    }
}
