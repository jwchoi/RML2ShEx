package rml2shex.model.shex;

public abstract class TripleExpr {
    enum Kinds { EachOf, OneOf, TripleConstraint, tripleExprRef }

    private Kinds kind;

    private String serializedTripleExpr;

    TripleExpr(Kinds kind) { this.kind = kind; }

    String getSerializedTripleExpr() { return serializedTripleExpr; }
    void setSerializedTripleExpr(String serializedTripleExpr) { this.serializedTripleExpr = serializedTripleExpr; }
}
