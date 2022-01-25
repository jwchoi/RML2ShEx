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
        // To omit "AND"
        if (shapeExprs.size() == 2) {
            List<ShapeExpr> shapeExprList = shapeExprs.stream().sorted().collect(Collectors.toList());
            if (shapeExprList.get(0).getKind().equals(Kinds.NodeConstraint) && shapeExprList.get(1).getKind().equals(Kinds.Shape)) {
                return shapeExprs.stream()
                        .map(ShapeExpr::getSerializedShapeExpr)
                        .sorted()
                        .collect(Collectors.joining(Symbols.SPACE));
            }
        }

        // general case
        return shapeExprs.stream()
                .map(ShapeExpr::getSerializedShapeExpr)
                .sorted()
                .collect(Collectors.joining(Symbols.SPACE + Symbols.AND + Symbols.SPACE));
    }
}
