package rml2shex.mapping.model.shex;

public abstract class ShapeExpr {
    enum Kinds { ShapeOr, ShapeAnd, ShapeNot, NodeConstraint, Shape, ShapeExternal, shapeExprRef }

    private Kinds kind;

    private String serializedShapeExpr;

    ShapeExpr(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }

    public String getSerializedShapeExpr() { return serializedShapeExpr; }
    void setSerializedShapeExpr(String serializedShapeExpr) { this.serializedShapeExpr = serializedShapeExpr; }

    @Override
    public String toString() {
        if (serializedShapeExpr == null) serializedShapeExpr = getSerializedShapeExpr();

        return serializedShapeExpr;
    }
}
