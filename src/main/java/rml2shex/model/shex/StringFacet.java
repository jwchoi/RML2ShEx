package rml2shex.model.shex;

import rml2shex.commons.Symbols;
import rml2shex.datasource.Column;
import rml2shex.model.rml.Template;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class StringFacet extends XSFacet {

    enum Kinds { REGEXP, STRING_LENGTH }

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

    private Kinds kind;

    // REGEXP
    private Optional<String> pattern;
    private Optional<String> flags;

    // stringLength INTEGER
    private Optional<StringLength> stringLength;
    private Optional<Integer> INTEGER;

    private StringFacet() {
        super(XSFacet.Kinds.stringFacet);

        pattern = Optional.empty();
        flags = Optional.empty();

        stringLength = Optional.empty();
        INTEGER = Optional.empty();
    }

    StringFacet(Template template) {
        this();
        kind = Kinds.REGEXP;
        setPattern(template);
    }

    StringFacet(StringLength stringLength, int INTEGER) {
        super(XSFacet.Kinds.stringFacet);
        kind = Kinds.STRING_LENGTH;
        this.stringLength = Optional.of(stringLength);
        this.INTEGER = Optional.of(INTEGER);
    }

    private void setPattern(Template template) {
        String pattern = template.getFormat();

        pattern = pattern.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        pattern = pattern.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        // logical references
        List<Column> logicalReferences = template.getLogicalReferences();
        for (Column logicalReference: logicalReferences) {
            String columnName = logicalReference.getName();
            String quantifier = logicalReference.getMinLength().isPresent() ? "{" + logicalReference.getMinLength().get() + ",}" : "*";
            pattern = pattern.replace("{" + columnName + "}", "(." + quantifier + ")");
        }

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

        if (isEquivalentPattern(o.pattern)) {
            if (Arrays.compare(flags.orElse("").toCharArray(), o.flags.orElse("").toCharArray()) == 0)
                return true;
        }

        return false;
    }

    private boolean isEquivalentPattern(Optional<String> pattern) {
        if (this.pattern.isEmpty() || pattern.isEmpty()) return false;

        String normalizedThisPattern = this.pattern.get().replaceAll("\\(\\.\\{\\d+\\,\\}\\)", "(.*)");
        String normalizedOtherPattern = pattern.get().replaceAll("\\(\\.\\{\\d+\\,\\}\\)", "(.*)");

        return normalizedThisPattern.equals(normalizedOtherPattern);
    }

    @Override
    public int compareTo(XSFacet o) {
        int resultFromSuper = super.compareTo(o);
        if (resultFromSuper != 0) return resultFromSuper;

        StringFacet other = (StringFacet) o;
        int resultFromThis = Integer.compare(kind.ordinal(), other.kind.ordinal());
        if (resultFromThis != 0) return resultFromThis;

        if (kind.equals(Kinds.REGEXP)) return toString().compareTo(other.toString());

        return Integer.compare(stringLength.get().ordinal(), other.stringLength.get().ordinal());
    }
}
