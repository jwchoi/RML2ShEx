package rml2shex.mapping.model.rml;

public class LanguageMap extends TermMap {
    @Override
    void setReference(String reference) {
        if (reference != null) {
            super.setColumn(reference);
            setTermType(TermTypes.LITERAL);
        }
    }

    @Override
    void setTemplate(Template template) {
        if (template != null) {
            super.setTemplate(template);
            setTermType(TermTypes.LITERAL);
        }
    }
}
