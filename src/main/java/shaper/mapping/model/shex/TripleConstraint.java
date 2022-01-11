package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.Optional;

public abstract class TripleConstraint implements Comparable<TripleConstraint> {

    enum MappedTypes { COLUMN, REF_CONSTRAINT, TABLE, RR_CLASSES, PREDICATE_OBJECT_MAP, REF_OBJECT_MAP }

    private ID id;

    private String serializedTripleConstraint;

    private MappedTypes mappedType;

    private String cardinality;

    private Optional<Boolean> isInverse = Optional.empty();

    TripleConstraint(MappedTypes mappedType) { this.mappedType = mappedType; }

    public ID getID() { return id; }

    protected String getSerializedTripleConstraint() { return serializedTripleConstraint; }

    protected void setSerializedTripleConstraint(String serializedTripleConstraint) {
        this.serializedTripleConstraint = serializedTripleConstraint;
    }

    protected String getCardinality() { return cardinality; }
    protected void setCardinality(String cardinality) { this.cardinality = cardinality; }

    Optional<Boolean> isInverse() { return isInverse; }
    protected void setIsInverse(boolean isInverse) { this.isInverse = Optional.of(isInverse); }

    MappedTypes getMappedType() { return mappedType; }

    @Override
    public int compareTo(TripleConstraint o) {
        return toString().compareTo(o.toString());
    }
}