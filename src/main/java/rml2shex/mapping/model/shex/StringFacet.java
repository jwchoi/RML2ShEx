package rml2shex.mapping.model.shex;

import rml2shex.util.Symbols;
import rml2shex.mapping.model.rml.Template;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class StringFacet extends XSFacet {
    private Optional<String> pattern;
    private Optional<String> flags;

    private StringFacet() {
        super(Kinds.stringFacet);

        pattern = Optional.empty();
        flags = Optional.empty();
    }

    StringFacet(Template template) {
        this();

        setPattern(template);
    }

    private void setPattern(Template template) {
        String pattern = template.getFormat();

        pattern = pattern.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        pattern = pattern.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        // logical references
        List<String> logicalReferences = template.getLogicalReferences();
        for (String logicalReference: logicalReferences)
            pattern = pattern.replace("{" + logicalReference + "}", "(.*)");

        pattern = Symbols.SLASH + Symbols.CARET + pattern + Symbols.DOLLAR + Symbols.SLASH;

        setPattern(pattern);
    }

    void setPattern(String pattern) { if (pattern != null) this.pattern = Optional.of(pattern); }

    private String getSerializedStringFacet() {
        StringBuffer sb = new StringBuffer();

        sb.append(pattern.orElse(""));
        sb.append(flags.orElse(""));

        return sb.toString();
    }

    @Override
    public String toString() { return getSerializedStringFacet(); }

    @Override
    boolean isEquivalent(XSFacet other) {
        if (!super.isEquivalent(other)) return false;

        StringFacet o = (StringFacet) other;

        if (pattern.equals(o.pattern)) {
            if (Arrays.compare(flags.orElse("").toCharArray(), o.flags.orElse("").toCharArray()) == 0)
                return true;
        }

        return false;
    }
}
