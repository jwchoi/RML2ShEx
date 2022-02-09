package rml2shex.model.shex;

public abstract class XSFacet implements Comparable<XSFacet> {
    enum Kinds { numericalFacet, stringFacet }

    private Kinds kind;

    XSFacet(Kinds kind) { this.kind = kind; }

    boolean isEquivalent(XSFacet other) { return kind.equals(other.kind); }

    @Override
    public int compareTo(XSFacet o) { return Integer.compare(kind.ordinal(), o.kind.ordinal()); }
}
