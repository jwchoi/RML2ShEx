package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ShapeAnd extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;

        private static int getPostfix() {
            return incrementer++;
        }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Set<ShapeExpr> shapeExprs;

    ShapeAnd(IRI id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(Kinds.ShapeAnd, id);

        shapeExprs = new HashSet<>();
        shapeExprs.add(shapeExpr1);
        shapeExprs.add(shapeExpr2);
    }

    void addShapeExpr(ShapeExpr shapeExpr) {
        shapeExprs.add(shapeExpr);
    }

    @Override
    public String getSerializedShapeExpr() {
        String serializedShapeExpr = super.getSerializedShapeExpr();
        if (serializedShapeExpr != null) return serializedShapeExpr;

        StringBuffer sb = new StringBuffer();

        List<ShapeExpr> shapeExprs = this.shapeExprs.stream().sorted().collect(Collectors.toList());

        ShapeExpr theFirstShapeExpr = shapeExprs.remove(0);

        sb.append(theFirstShapeExpr.getSerializedShapeExpr());

        List<ShapeExpr> theRestShapeExprs = shapeExprs;

        for (ShapeExpr shapeExpr : theRestShapeExprs) {
            sb.append(Symbols.SPACE + Symbols.AND + Symbols.SPACE);
            sb.append(shapeExpr.getSerializedShapeExpr());
        }

        // To omit the last "AND"
        if (theRestShapeExprs.size() == 1) {
            if (theFirstShapeExpr.getKind().equals(Kinds.NodeConstraint) && theRestShapeExprs.get(0).getKind().equals(Kinds.Shape)) {
                String omission = Symbols.AND + Symbols.SPACE;
                int start = sb.lastIndexOf(omission);
                int end = start + omission.length();
                sb.delete(start, end);
            }
        }

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
