package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public abstract class Shape extends ShapeExpr implements Comparable<Shape> {
    private Optional<ID> id;
    private String serializedShape;
    private Set<TripleExpr> tripleExprs;

    private Shape() {
        super(Kinds.Shape);

        id = Optional.empty();
        tripleExprs = new HashSet<>();
    }

    Shape(ID id) {
        this();
        this.id = Optional.ofNullable(id);
    }

    ID getID() { return id.isPresent() ? id.get() : null; }
    void setID(ID id) { this.id = Optional.ofNullable(id); }

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

    void addTripleExpr(TripleExpr tripleExpr) {
        tripleExprs.add(tripleExpr);
    }

    Set<TripleExpr> getTripleExprs() { return tripleExprs; }
}
