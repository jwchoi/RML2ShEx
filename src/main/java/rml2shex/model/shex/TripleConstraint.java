package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.model.rml.ObjectMap;
import rml2shex.model.rml.PredicateMap;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TripleConstraint extends DeclarableTripleExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    enum MappedTypes { CLASS, PREDICATE_OBJECT_MAP, PREDICATE_REF_OBJECT_MAP }

    private MappedTypes mappedType;

    private String cardinality;

    private Optional<Boolean> isInverse = Optional.empty();

    private TripleConstraint(IRI id, MappedTypes mappedType) {
        super(Kinds.TripleConstraint, id);
        this.mappedType = mappedType;
    }

    TripleConstraint(IRI id, Set<URI> classes) {
        this(id, MappedTypes.CLASS);
    }

    TripleConstraint(IRI id, PredicateMap predicateMap, ObjectMap objectMap) {
        this(id, MappedTypes.PREDICATE_OBJECT_MAP);

    }

    TripleConstraint(IRI id, PredicateMap predicateMap, IRI shapeExprIdAsObject) {
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