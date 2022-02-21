package rml2shex.model.rml;

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

    @Override
    void setConstant(String constant) throws Exception {
        if (constant != null) {
            if (constant.matches("[a-zA-Z]+(\\-[a-zA-Z0-9]+)*")) super.setConstant(constant);
            else throw new Exception("A term map with invalid rr:language value, which is an error.");
        }
    }
}
