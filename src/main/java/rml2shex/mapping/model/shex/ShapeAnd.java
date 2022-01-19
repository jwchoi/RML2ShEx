package rml2shex.mapping.model.shex;

import rml2shex.util.Id;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ShapeAnd extends ShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Optional<Id> id;
    private Set<ShapeExpr> shapeExprs;

    ShapeAnd(Id id, ShapeExpr shapeExpr1, ShapeExpr shapeExpr2) {
        super(Kinds.ShapeAnd);

        this.id = Optional.ofNullable(id);

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

        String id = this.id.isPresent() ? this.id.get().getPrefixedName() : Symbols.EMPTY;
        sb.append(id);

        List<ShapeExpr> shapeExprs = this.shapeExprs.stream().collect(Collectors.toList());

        sb.append(Symbols.SPACE + shapeExprs.remove(0).getSerializedShapeExpr());

        for (ShapeExpr shapeExpr: shapeExprs) {
            sb.append(Symbols.SPACE + Symbols.AND + Symbols.SPACE);
            sb.append(shapeExpr.getSerializedShapeExpr());
        }

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
