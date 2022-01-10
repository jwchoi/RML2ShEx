package shaper.mapping.model.shex;

import java.util.HashSet;
import java.util.Set;

public class Shape extends ShapeExpr implements Comparable<Shape> {
    private String id;
    private String serializedShape;

    private Set<TripleConstraint> tripleConstraints;

    Shape(String id) {
        this.id = id;

        tripleConstraints = new HashSet<>();
    }

    String getShapeID() { return id; }

    @Override
    public int compareTo(Shape o) {
        return id.compareTo(o.getShapeID());
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
