package rml2shex.mapping.model.shex;

import rml2shex.util.Id;
import rml2shex.mapping.model.rml.ObjectMap;
import rml2shex.mapping.model.rml.PredicateMap;
import rml2shex.mapping.model.rml.RefObjectMap;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

public class TripleConstraint extends TripleExpr implements Comparable<TripleConstraint> {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    enum MappedTypes { CLASS, PREDICATE_OBJECT_MAP, PREDICATE_REF_OBJECT_MAP }

    private Optional<Id> id;

    private String serializedTripleConstraint;

    private MappedTypes mappedType;

    private String cardinality;

    private Optional<Boolean> isInverse = Optional.empty();

    private TripleConstraint(MappedTypes mappedType) {
        super(Kinds.TripleConstraint);
        this.mappedType = mappedType;
    }

    private TripleConstraint(Id id, MappedTypes mappedType) {
        this(mappedType);
        this.id = Optional.ofNullable(id);
    }

    TripleConstraint(Id id, Set<URI> classes) {
        this(id, MappedTypes.CLASS);
    }

    TripleConstraint(Id id, PredicateMap predicateMap, ObjectMap objectMap) {
        this(id, MappedTypes.PREDICATE_OBJECT_MAP);
    }

    TripleConstraint(Id id, PredicateMap predicateMap, RefObjectMap refObjectMap) {
        this(id, MappedTypes.PREDICATE_REF_OBJECT_MAP);
    }

    public Id getID() { return id.isPresent() ? id.get() : null; }

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