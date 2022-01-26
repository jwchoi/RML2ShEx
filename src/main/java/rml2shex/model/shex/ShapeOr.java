package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ShapeOr extends DeclarableShapeExpr {

    private Set<ShapeExpr> shapeExprs;

    ShapeOr(IRI id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(Kinds.ShapeOr, id);

        shapeExprs = new HashSet<>();
        shapeExprs.add(shapeExpr1);
        shapeExprs.add(shapeExpr2);
    }

    void addShapeExpr(ShapeExpr shapeExpr) { shapeExprs.add(shapeExpr); }

    @Override
    public String getSerializedShapeExpr() {
        StringBuffer sb = new StringBuffer(super.getSerializedShapeExpr());

        String shapeExpression = shapeExprs.stream()
                .map(ShapeExpr::getSerializedShapeExpr)
                .sorted()
                .collect(Collectors.joining(Symbols.SPACE + Symbols.OR + Symbols.SPACE));

        sb.append(shapeExpression);

        return sb.toString();
    }
}
