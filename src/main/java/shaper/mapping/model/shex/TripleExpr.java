package shaper.mapping.model.shex;

public abstract class TripleExpr {
    enum Kinds { EachOf, OneOf, TripleConstraint, tripleExprRef }

    private Kinds kind;

    TripleExpr(Kinds kind) { this.kind = kind; }
}
