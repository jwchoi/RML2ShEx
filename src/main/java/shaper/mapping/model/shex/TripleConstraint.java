package shaper.mapping.model.shex;

import shaper.mapping.model.ID;

import java.util.Optional;

public abstract class TripleConstraint extends TripleExpr implements Comparable<TripleConstraint> {
    private static int incrementer = 0;

    static int getIncrementer() { return incrementer++; }

    enum MappedTypes { COLUMN, REF_CONSTRAINT, TABLE, CLASS, PREDICATE_OBJECT_MAP, PREDICATE_REF_OBJECT_MAP }

    private Optional<ID> id;

    private String serializedTripleConstraint;

    private MappedTypes mappedType;

    private String cardinality;

    private Optional<Boolean> isInverse = Optional.empty();

    TripleConstraint(MappedTypes mappedType) {
        super(Kinds.TripleConstraint);
        this.mappedType = mappedType;
    }

    TripleConstraint(ID id, MappedTypes mappedType) {
        this(mappedType);
        this.id = Optional.ofNullable(id);
    }

    public ID getID() { return id.isPresent() ? id.get() : null; }

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