package rml2shex.model.shex;

import rml2shex.util.Id;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.Optional;

public class Shape extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Optional<TripleExpr> expression;

    Shape(Id id) {
        super(Kinds.Shape, id);
        expression = Optional.empty();
    }

    Shape(Id id, TripleExpr tripleExpr) {
        this(id);

        expression = Optional.ofNullable(tripleExpr);
    }

    @Override
    public String getSerializedShapeExpr() {
        String serializedShapeExpr = super.getSerializedShapeExpr();
        if (serializedShapeExpr != null) return serializedShapeExpr;

        if (expression.isEmpty()) return Symbols.EMPTY;

        StringBuffer sb = new StringBuffer();

        sb.append(Symbols.OPEN_BRACE + Symbols.NEWLINE);
        sb.append(Symbols.SPACE + Symbols.SPACE + expression.get().getSerializedTripleExpr() + Symbols.NEWLINE);
        sb.append(Symbols.CLOSE_BRACE);

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
