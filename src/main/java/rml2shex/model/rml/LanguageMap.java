package rml2shex.model.rml;

import org.apache.commons.lang3.LocaleUtils;

import java.util.Locale;

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
            Locale locale = new Locale.Builder().setLanguageTag(constant).build();

            if (LocaleUtils.isAvailableLocale(locale)) super.setConstant(constant);
            else throw new Exception("A term map with invalid rr:language value, which is an error.");
        }
    }
}
