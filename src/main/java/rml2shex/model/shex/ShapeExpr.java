package rml2shex.model.shex;

public abstract class ShapeExpr implements Comparable<ShapeExpr> {
    enum Kinds { NodeConstraint, Shape, ShapeAnd, ShapeOr, ShapeNot, ShapeExternal, shapeExprRef }

    private Kinds kind;

    private String serializedShapeExpr;

    ShapeExpr(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }

    String getSerializedShapeExpr() { return serializedShapeExpr; }
    void setSerializedShapeExpr(String serializedShapeExpr) { this.serializedShapeExpr = serializedShapeExpr; }

    @Override
    public String toString() {
        if (serializedShapeExpr == null) serializedShapeExpr = getSerializedShapeExpr();

        return serializedShapeExpr;
    }

    @Override
    public int compareTo(ShapeExpr o) {
        return kind.ordinal() - o.kind.ordinal();
    }
}
