package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.Optional;

public abstract class DeclarableTripleExpr extends TripleExpr {
    private Optional<IRI> id;

    DeclarableTripleExpr(Kinds kind, IRI id) {
        super(kind);
        this.id = Optional.ofNullable(id);
    }

    IRI getId() { return id.isPresent() ? id.get() : null; }

    public String getTripleExprDecl() { return id.get().getPrefixedName() + Symbols.SPACE + getSerializedTripleExpr(); }
}
