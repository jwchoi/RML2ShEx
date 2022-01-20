package rml2shex.model.shex;

import rml2shex.util.Id;
import rml2shex.util.Symbols;

import java.util.Optional;

public class DeclarableTripleExpr extends TripleExpr {
    private Optional<Id> id;

    DeclarableTripleExpr(Kinds kind, Id id) {
        super(kind);
        this.id = Optional.ofNullable(id);
    }

    Id getId() { return id.isPresent() ? id.get() : null; }

    public String getTripleExprDecl() { return id.get().getPrefixedName() + Symbols.SPACE + getSerializedTripleExpr(); }
}
