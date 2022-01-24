package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ShapeOr extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Set<ShapeExpr> shapeExprs;

    ShapeOr(IRI id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(Kinds.ShapeOr, id);

        shapeExprs = new HashSet<>();
        shapeExprs.add(shapeExpr1);
        shapeExprs.add(shapeExpr2);
    }

    void addShapeExpr(ShapeExpr shapeExpr) { shapeExprs.add(shapeExpr); }

    @Override
    public String getSerializedShapeExpr() {
        String serializedShapeExpr = super.getSerializedShapeExpr();
        if (serializedShapeExpr != null) return serializedShapeExpr;

        serializedShapeExpr = shapeExprs.stream()
                .map(ShapeExpr::getSerializedShapeExpr)
                .sorted()
                .collect(Collectors.joining(Symbols.SPACE + Symbols.OR + Symbols.SPACE));

        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
