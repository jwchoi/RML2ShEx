package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ShapeAnd extends ShapeExpr {

    private static int incrementer = 0;
    static int getIncrementer() { return incrementer++; }

    private Optional<ID> id;
    private Set<ShapeExpr> shapeExprs;

    ShapeAnd(ID id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(Kinds.ShapeAnd);

        this.id = Optional.ofNullable(id);

        shapeExprs = new HashSet<>();
        shapeExprs.add(shapeExpr1);
        shapeExprs.add(shapeExpr2);
    }
}
