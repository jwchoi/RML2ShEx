package shaper.mapping.model.rml;

public class LanguageMap extends TermMap {
    @Override
    void setReference(String reference) {
        if (reference != null) {
            super.setColumn(reference);
            setTermType(TermTypes.LITERAL);
        }
    }
}
