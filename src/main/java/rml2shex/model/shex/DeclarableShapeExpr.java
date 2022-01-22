package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.Optional;

public class DeclarableShapeExpr extends ShapeExpr {
    private Optional<IRI> id;

    DeclarableShapeExpr(Kinds kind, IRI id) {
        super(kind);
        this.id = Optional.ofNullable(id);
    }

    IRI getId() { return id.isPresent() ? id.get() : null; }

    public String getShapeExprDecl() { return id.get().getPrefixedName() + Symbols.SPACE + getSerializedShapeExpr(); }
}
