package rml2shex.mapping.model.shex;

import rml2shex.util.Id;

import java.util.Optional;
import java.util.Set;

public class Shape extends ShapeExpr implements Comparable<Shape> {

    private static int incrementer = 0;
    static int getIncrementer() { return incrementer++; }

    private Optional<Id> id;
    private String serializedShape;

    private Optional<TripleExpr> expression;

    private Shape() {
        super(Kinds.Shape);

        id = Optional.empty();
        expression = Optional.empty();
    }

    Shape(Id id) {
        this();
        this.id = Optional.ofNullable(id);
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

    Id getID() { return id.isPresent() ? id.get() : null; }

    @Override
    public int compareTo(Shape o) {
        return getID().compareTo(o.getID());
    }

    protected String getSerializedShape() {
        return serializedShape;
    }

    protected void setSerializedShape(String serializedShape) {
        this.serializedShape = serializedShape;
    }
}
