package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.Optional;

public class Shape extends DeclarableShapeExpr {

    private Optional<Boolean> closed;
    private Optional<TripleExpr> expression;

    private Shape(IRI id) {
        super(Kinds.Shape, id);
        closed = Optional.empty();
        expression = Optional.empty();
    }

    Shape(IRI id, TripleExpr tripleExpr) {
        this(id);
        expression = Optional.ofNullable(tripleExpr);
    }

    Shape(boolean closed, TripleExpr tripleExpr) { this(null, closed, tripleExpr); }

    Shape(IRI id, boolean closed, TripleExpr tripleExpr) {
        this(id, tripleExpr);
        this.closed = Optional.of(closed);
    }

    @Override
    public String getSerializedShapeExpr() {
        if (expression.isEmpty()) return Symbols.EMPTY;

        StringBuffer sb = new StringBuffer();

        sb.append(closed.isPresent() ? (closed.get().booleanValue() ? Symbols.CLOSED + Symbols.SPACE : Symbols.EMPTY) : Symbols.EMPTY);
        sb.append(Symbols.OPEN_BRACE + Symbols.NEWLINE);
        sb.append(expression.get().getSerializedTripleExpr().indent(2));
        sb.append(Symbols.CLOSE_BRACE);

        return sb.toString();
    }
}
