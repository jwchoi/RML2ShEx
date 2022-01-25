package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.Optional;

public class Shape extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Optional<TripleExpr> expression;

    Shape(IRI id) {
        super(Kinds.Shape, id);
        expression = Optional.empty();
    }

    Shape(IRI id, TripleExpr tripleExpr) {
        this(id);

        expression = Optional.ofNullable(tripleExpr);
    }

    @Override
    public String getSerializedShapeExpr() {
        if (expression.isEmpty()) return Symbols.EMPTY;

        StringBuffer sb = new StringBuffer();

        sb.append(Symbols.OPEN_BRACE + Symbols.NEWLINE);
        sb.append(expression.get().getSerializedTripleExpr().indent(2));
        sb.append(Symbols.CLOSE_BRACE);

        return sb.toString();
    }
}
