package rml2shex.model.shex;

import rml2shex.util.IRI;

import java.util.Optional;

public class ObjectValue extends ValueSetValue {

    enum Kinds { IRIREF, ObjectLiteral }

    private class IRIREF extends ObjectValue {
        private Optional<IRI> IRIREF;
        private IRIREF(IRI IRIREF) { this.IRIREF = Optional.ofNullable(IRIREF); }
    }
    private class ObjectLiteral extends ObjectValue {}

    private Kinds kind;

    Optional<IRIREF> IRIREF;

    private ObjectValue() {
        super(ValueSetValue.Kinds.objectValue);
        IRIREF = Optional.empty();
    }

    private ObjectValue(Kinds kind) {
        this();
        this.kind = kind;
    }

    ObjectValue(IRI IRIREF) {
        this(Kinds.IRIREF);
        this.IRIREF = Optional.of(new IRIREF(IRIREF));
    }
}
