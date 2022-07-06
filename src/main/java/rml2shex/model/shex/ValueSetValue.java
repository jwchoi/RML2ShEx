package rml2shex.model.shex;

import org.jetbrains.annotations.NotNull;
import rml2shex.commons.IRI;
import rml2shex.commons.Symbols;

import java.util.Objects;
import java.util.Optional;

public abstract class ValueSetValue {
    enum Kinds { objectValue, IriStem, IriStemRange, LiteralStem, LiteralStemRange, Language, LanguageStem, LanguageStemRange }

    private Kinds kind;

    ValueSetValue(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }

    abstract String getSerializedValueSetValue();

    @Override
    public String toString() { return getSerializedValueSetValue(); }

    public static abstract class ObjectValue extends ValueSetValue {

        enum Kinds { IRIREF, ObjectLiteral }

        static class IRIREF extends ObjectValue {
            private Optional<IRI> IRIREF;

            IRIREF(IRI IRIREF) {
                super(ObjectValue.Kinds.IRIREF);
                this.IRIREF = Optional.ofNullable(IRIREF);
            }

            @Override
            String getSerializedValueSetValue() {
                return IRIREF.isPresent() ? IRIREF.get().getPrefixedNameOrElseAbsoluteIRI() : Symbols.EMPTY;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof IRIREF)) return false;
                IRIREF iriref = (IRIREF) o;
                return Objects.equals(IRIREF, iriref.IRIREF);
            }

            @Override
            public int hashCode() { return Objects.hash(IRIREF); }
        }

        static class ObjectLiteral extends ObjectValue {
            private String value;
//            private Optional<String> language;
//            private Optional<String> type;

            ObjectLiteral(String value) {
                super(ObjectValue.Kinds.ObjectLiteral);
                this.value = value;
            }

            @Override
            String getSerializedValueSetValue() { return value; }
        }

        private Kinds kind;

        ObjectValue(Kinds kind) {
            super(ValueSetValue.Kinds.objectValue);
            this.kind = kind;
        }
    }

    public static class Language extends ValueSetValue {
        private String lanaguageTag;

        Language(String LANGTAG) {
            super(Kinds.Language);
            lanaguageTag = LANGTAG;
        }

        @Override
        String getSerializedValueSetValue() { return Symbols.AT + lanaguageTag; }
    }
}
