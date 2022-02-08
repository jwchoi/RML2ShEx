package rml2shex.model.shex;

import rml2shex.commons.Symbols;
import rml2shex.datasource.Column;
import rml2shex.model.rml.Template;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StringFacet extends XSFacet {

    enum StringLength {
        LENGTH("LENGTH"), MIN_LENGTH("MINLENGTH"), MAX_LENGTH("MAXLENGTH");

        private final String stringLength;

        StringLength(String stringLength) {
            this.stringLength = stringLength;
        }

        @Override
        public String toString() {
            return stringLength;
        }
    }

    // stringFacet ::= stringLength INTEGER | REGEXP

    // REGEXP
    private Optional<String> pattern;
    private Optional<String> flags;

    // stringLength INTEGER
    private Optional<StringLength> stringLength;
    private Optional<Integer> INTEGER;

    private StringFacet() {
        super(Kinds.stringFacet);

        pattern = Optional.empty();
        flags = Optional.empty();

        stringLength = Optional.empty();
        INTEGER = Optional.empty();
    }

    StringFacet(Template template) {
        this();

        setPattern(template);
    }

    StringFacet(StringLength stringLength, int INTEGER) {
        super(Kinds.stringFacet);

        this.stringLength = Optional.of(stringLength);
        this.INTEGER = Optional.of(INTEGER);
    }

    private void setPattern(Template template) {
        String pattern = template.getFormat();

        pattern = pattern.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        pattern = pattern.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        // logical references
        List<String> logicalReferences = template.getLogicalReferences().stream().map(Column::getName).collect(Collectors.toList());
        for (String logicalReference: logicalReferences)
            pattern = pattern.replace("{" + logicalReference + "}", "(.*)");

        pattern = Symbols.SLASH + Symbols.CARET + pattern + Symbols.DOLLAR + Symbols.SLASH;

        setPattern(pattern);
    }

    void setPattern(String pattern) { if (pattern != null) this.pattern = Optional.of(pattern); }

    private String getSerializedStringFacet() {
        if (stringLength.isPresent() && INTEGER.isPresent()) { return stringLength.get() + Symbols.SPACE + INTEGER.get(); }

        if (pattern.isPresent()) return pattern.get() + flags.orElse("");

        return Symbols.EMPTY;
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
