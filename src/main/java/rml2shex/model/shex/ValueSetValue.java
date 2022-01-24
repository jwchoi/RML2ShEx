package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.Optional;

public abstract class ValueSetValue {
    enum Kinds { objectValue, IriStem, IriStemRange, LiteralStem, LiteralStemRange, Language, LanguageStem, LanguageStemRange }

    private Kinds kind;

    private String serializedValueSetValue;

    ValueSetValue(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }

    String getSerializedValueSetValue() { return serializedValueSetValue; }
    void setSerializedValueSetValue(String serializedValueSetValue) { this.serializedValueSetValue = serializedValueSetValue; }

    @Override
    public String toString() {
        if (serializedValueSetValue == null) serializedValueSetValue = getSerializedValueSetValue();

        return serializedValueSetValue;
    }

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
                String serializedValueSetValue = super.getSerializedValueSetValue();
                if (serializedValueSetValue != null) return serializedValueSetValue;

                serializedValueSetValue = IRIREF.isPresent() ? IRIREF.get().getPrefixedNameOrElseAbsoluteIRI() : Symbols.EMPTY;

                setSerializedValueSetValue(serializedValueSetValue);
                return serializedValueSetValue;
            }
        }

        static class ObjectLiteral extends ObjectValue {
            ObjectLiteral() { super(ObjectValue.Kinds.ObjectLiteral); }
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
            lanaguageTag = Symbols.AT + LANGTAG;
        }

        @Override
        String getSerializedValueSetValue() {
            String serializedValueSetValue = super.getSerializedValueSetValue();
            if (serializedValueSetValue != null) return serializedValueSetValue;

            serializedValueSetValue = lanaguageTag;

            setSerializedValueSetValue(serializedValueSetValue);
            return serializedValueSetValue;
        }
    }
}
