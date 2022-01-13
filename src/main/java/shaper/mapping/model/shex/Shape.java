package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public abstract class Shape extends ShapeExpr implements Comparable<Shape> {

    private static int incrementer = 0;
    static int getIncrementer() { return incrementer++; }

    private Optional<ID> id;
    private String serializedShape;
    private Set<TripleConstraint> tripleConstraints;

    private Optional<TripleExpr> expression;

    private Shape() {
        super(Kinds.Shape);

        id = Optional.empty();
        tripleConstraints = new HashSet<>();
    }

    Shape(ID id) {
        this();
        this.id = Optional.ofNullable(id);
    }

    Shape(ID id, TripleExpr tripleExpr) {
        this(id);

        expression = Optional.ofNullable(tripleExpr);
    }

    ID getID() { return id.isPresent() ? id.get() : null; }

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

    void addTripleConstraint(TripleConstraint tripleConstraint) {
        this.tripleConstraints.add(tripleConstraint);
    }

    Set<TripleConstraint> getTripleConstraints() { return tripleConstraints; }
}
