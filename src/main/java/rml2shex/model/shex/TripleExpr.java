package rml2shex.model.shex;

public abstract class TripleExpr {
    enum Kinds { EachOf, OneOf, TripleConstraint, tripleExprRef }

    private Kinds kind;

    TripleExpr(Kinds kind) { this.kind = kind; }

    abstract String getSerializedTripleExpr();
}
