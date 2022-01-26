package rml2shex.model.shex;

public abstract class ShapeExpr implements Comparable<ShapeExpr> {
    enum Kinds { NodeConstraint, Shape, ShapeAnd, ShapeOr, ShapeNot, ShapeExternal, shapeExprRef }

    private Kinds kind;

    ShapeExpr(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }


    public abstract String getSerializedShapeExpr();

    @Override
    public String toString() { return getSerializedShapeExpr(); }

    @Override
    public int compareTo(ShapeExpr o) {
        return kind.ordinal() - o.kind.ordinal();
    }
}
