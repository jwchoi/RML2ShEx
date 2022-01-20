package rml2shex.model.shex;

import rml2shex.util.Id;
import rml2shex.model.rml.ObjectMap;
import rml2shex.model.rml.PredicateMap;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

public class TripleConstraint extends DeclarableTripleExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    enum MappedTypes { CLASS, PREDICATE_OBJECT_MAP, PREDICATE_REF_OBJECT_MAP }

    private MappedTypes mappedType;

    private String cardinality;

    private Optional<Boolean> isInverse = Optional.empty();

    private TripleConstraint(Id id, MappedTypes mappedType) {
        super(Kinds.TripleConstraint, id);
        this.mappedType = mappedType;
    }

    TripleConstraint(Id id, Set<URI> classes) {
        this(id, MappedTypes.CLASS);
    }

    TripleConstraint(Id id, PredicateMap predicateMap, ObjectMap objectMap) {
        this(id, MappedTypes.PREDICATE_OBJECT_MAP);

    }

    TripleConstraint(Id id, PredicateMap predicateMap, Id shapeExprIdAsObject) {
        this(id, MappedTypes.PREDICATE_REF_OBJECT_MAP);
    }

    protected String getCardinality() { return cardinality; }
    protected void setCardinality(String cardinality) { this.cardinality = cardinality; }

    Optional<Boolean> isInverse() { return isInverse; }
    protected void setIsInverse(boolean isInverse) { this.isInverse = Optional.of(isInverse); }

    MappedTypes getMappedType() { return mappedType; }

    private void convert(PredicateMap predicateMap, ObjectMap objectMap) {

    }

    @Override
    String getSerializedTripleExpr() {
        return "dummy";
    }
}