package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

public abstract class ShapeExpr {
    private ID id;

    ShapeExpr(ID id) { this.id = id; }

    public ID getID() { return id; }
}
