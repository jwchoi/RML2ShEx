package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.Optional;

public abstract class DeclarableShapeExpr extends ShapeExpr {
    private Optional<IRI> id;

    DeclarableShapeExpr(Kinds kind, IRI id) {
        super(kind);
        this.id = Optional.ofNullable(id);
    }

    IRI getId() { return id.isPresent() ? id.get() : null; }

    public String getShapeExprDecl() { return id.get().getPrefixedName() + Symbols.SPACE + getSerializedShapeExpr(); }

    @Override
    public int compareTo(ShapeExpr o) {
        if (!o.getKind().equals(Kinds.shapeExprRef)) {
            if (id.isPresent() && ((DeclarableShapeExpr) o).id.isPresent()) {
                return id.get().compareTo(((DeclarableShapeExpr) o).id.get());
            }
        }

        return super.compareTo(o);
    }
}
