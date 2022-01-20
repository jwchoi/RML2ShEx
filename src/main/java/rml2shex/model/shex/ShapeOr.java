package rml2shex.model.shex;

import rml2shex.util.Id;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ShapeOr extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Set<ShapeExpr> shapeExprs;

    ShapeOr(Id id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
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

        StringBuffer sb = new StringBuffer();

        List<ShapeExpr> shapeExprs = this.shapeExprs.stream().collect(Collectors.toList());

        sb.append(shapeExprs.remove(0).getSerializedShapeExpr());

        for (ShapeExpr shapeExpr: shapeExprs) {
            sb.append(Symbols.SPACE + Symbols.OR + Symbols.SPACE);
            sb.append(shapeExpr.getSerializedShapeExpr());
        }

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
