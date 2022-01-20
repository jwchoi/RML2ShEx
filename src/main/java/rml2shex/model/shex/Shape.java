package rml2shex.model.shex;

import rml2shex.util.Id;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

public class Shape extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private String serializedShape;

    private Optional<TripleExpr> expression;

    Shape(Id id) {
        super(Kinds.Shape, id);
        expression = Optional.empty();
    }

    Shape(Id id, TripleExpr tripleExpr) {
        this(id);

        expression = Optional.ofNullable(tripleExpr);
    }

    Shape(Id id, Set<TripleConstraint> tripleConstraints) {
        this(id);
    }

    private void setExpression(Set<TripleConstraint> tripleConstraints) {

    }

    protected String getSerializedShape() {
        return serializedShape;
    }

    protected void setSerializedShape(String serializedShape) {
        this.serializedShape = serializedShape;
    }
}
