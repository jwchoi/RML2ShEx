package rml2shex.mapping.model.shex;

import rml2shex.util.ID;

import java.util.Optional;

public class ShapeOr extends ShapeExpr {
    private Optional<ID> id;

    ShapeOr(ID id) {
        super(Kinds.ShapeOr);
        this.id = Optional.ofNullable(id);
    }
}
