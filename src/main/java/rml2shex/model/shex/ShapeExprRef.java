package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

public class ShapeExprRef extends ShapeExpr {
    private IRI shapeExprLabel;

    ShapeExprRef(IRI shapeExprLabel) {
        super(Kinds.shapeExprRef);
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
