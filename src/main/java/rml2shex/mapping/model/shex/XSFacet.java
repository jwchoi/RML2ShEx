package rml2shex.mapping.model.shex;

public abstract class XSFacet {
    enum Kinds { stringFacet, numericalFacet }

    private Kinds kind;

    XSFacet(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }

    boolean isEquivalent(XSFacet other) { return kind.equals(other.kind); }
}
