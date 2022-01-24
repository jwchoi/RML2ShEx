package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.Optional;

public class ObjectValue extends ValueSetValue {

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
