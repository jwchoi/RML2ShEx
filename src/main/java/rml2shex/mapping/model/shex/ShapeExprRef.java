package rml2shex.mapping.model.shex;

import rml2shex.util.Id;
import rml2shex.util.Symbols;

public class ShapeExprRef extends ShapeExpr {
    private Id shapeExprLabel;

    private ShapeExprRef() { super(Kinds.shapeExprRef); }

    ShapeExprRef(Id shapeExprLabel) {
        this();
        this.shapeExprLabel = shapeExprLabel;
    }

    @Override
    public String getSerializedShapeExpr() {
        String serializedShapeExpr = super.getSerializedShapeExpr();
        if (serializedShapeExpr != null) return serializedShapeExpr;

        serializedShapeExpr = Symbols.AT + shapeExprLabel.getPrefixedName();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
