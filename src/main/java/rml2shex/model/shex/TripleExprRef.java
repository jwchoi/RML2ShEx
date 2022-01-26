package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

public class TripleExprRef extends TripleExpr {
    private IRI tripleExprLabel;

    TripleExprRef(IRI tripleExprLabel) {
        super(Kinds.tripleExprRef);
        this.tripleExprLabel = tripleExprLabel;
    }

    @Override
    public String getSerializedTripleExpr() { return Symbols.AMPERSAND + tripleExprLabel.getPrefixedName(); }
}
