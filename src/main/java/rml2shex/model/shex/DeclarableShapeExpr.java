package rml2shex.model.shex;

import rml2shex.util.Id;
import rml2shex.util.Symbols;

import java.util.Optional;

public class DeclarableShapeExpr extends ShapeExpr {
    private Optional<Id> id;

    private DeclarableShapeExpr(Kinds kind) {
        super(kind);
        id = Optional.empty();
    }

    DeclarableShapeExpr(Kinds kind, Id id) {
        this(kind);
        this.id = Optional.ofNullable(id);
    }

    Id getId() { return id.isPresent() ? id.get() : null; }

    public String getShapeExprDecl() { return id.get().getPrefixedName() + Symbols.SPACE + getSerializedShapeExpr(); }
}
