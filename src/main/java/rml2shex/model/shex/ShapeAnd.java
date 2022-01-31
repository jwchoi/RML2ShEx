package rml2shex.model.shex;

import rml2shex.commons.IRI;
import rml2shex.commons.Symbols;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ShapeAnd extends DeclarableShapeExpr {

    private Set<ShapeExpr> shapeExprs;

    ShapeAnd(IRI id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(Kinds.ShapeAnd, id);

        shapeExprs = new HashSet<>();
        shapeExprs.add(shapeExpr1);
        shapeExprs.add(shapeExpr2);
    }

    void addShapeExpr(ShapeExpr shapeExpr) {
        shapeExprs.add(shapeExpr);
    }

    @Override
    public String getSerializedShapeExpr() {
        StringBuffer sb = new StringBuffer(super.getSerializedShapeExpr());

        String shapeExpression = shapeExprs.stream()
                .sorted()
                .map(ShapeExpr::getSerializedShapeExpr)
                .collect(Collectors.joining(Symbols.SPACE + Symbols.AND + Symbols.SPACE));

        sb.append(shapeExpression);

        return sb.toString();
    }
}
