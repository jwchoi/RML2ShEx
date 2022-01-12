package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.HashSet;
import java.util.Set;

public class ShapeAnd extends ShapeExpr {
    private Set<ShapeExpr> shapeExprs;

    ShapeAnd(ID id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(id);

        shapeExprs = new HashSet<>();
        shapeExprs.add(shapeExpr1);
        shapeExprs.add(shapeExpr2);
    }
}
