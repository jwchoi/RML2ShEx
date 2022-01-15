package rml2shex.mapping.model.shex;

public abstract class ShapeExpr {
    enum Kinds { ShapeOr, ShapeAnd, ShapeNot, NodeConstraint, Shape, ShapeExternal, shapeExprRef }

    private Kinds kind;

    ShapeExpr(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }
}
