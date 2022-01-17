package rml2shex.mapping.model.shex;

import rml2shex.util.Id;

import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

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
}
