package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.HashSet;
import java.util.Set;

public abstract class Shape extends ShapeExpr implements Comparable<Shape> {
    private String serializedShape;

    private Set<TripleConstraint> tripleConstraints;

    Shape(ID id) {
        super(id);

        tripleConstraints = new HashSet<>();
    }

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
        tripleConstraints.add(tripleConstraint);
    }

    Set<TripleConstraint> getTripleConstraints() { return tripleConstraints; }
}
