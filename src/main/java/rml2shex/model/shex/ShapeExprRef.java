package rml2shex.model.shex;

import rml2shex.commons.IRI;
import rml2shex.commons.Symbols;

public class ShapeExprRef extends ShapeExpr {
    private IRI shapeExprLabel;

    ShapeExprRef(IRI shapeExprLabel) {
        super(Kinds.shapeExprRef);
        this.shapeExprLabel = shapeExprLabel;
    }

    @Override
    public String getSerializedShapeExpr() { return Symbols.AT + shapeExprLabel.getPrefixedName(); }
}
