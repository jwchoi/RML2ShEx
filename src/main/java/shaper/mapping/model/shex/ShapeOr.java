package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.Optional;

public class ShapeOr extends ShapeExpr {
    private Optional<ID> id;

    ShapeOr(ID id) {
        super(Kinds.ShapeOr);
        this.id = Optional.ofNullable(id);
    }
}
