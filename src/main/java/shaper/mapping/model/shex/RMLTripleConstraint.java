package shaper.mapping.model.shex;

import shaper.mapping.model.ID;
import shaper.mapping.model.rml.ObjectMap;
import shaper.mapping.model.rml.PredicateMap;
import shaper.mapping.model.rml.RefObjectMap;

import java.net.URI;
import java.util.Set;

public class RMLTripleConstraint extends TripleConstraint {
    private RMLTripleConstraint(ID id, MappedTypes mappedType) {
        super(id, mappedType);
    }

    RMLTripleConstraint(ID id, Set<URI> classes) {
        this(id, MappedTypes.CLASS);
    }

    RMLTripleConstraint(ID id, PredicateMap predicateMap, ObjectMap objectMap) {
        this(id, MappedTypes.PREDICATE_OBJECT_MAP);
    }

    RMLTripleConstraint(ID id, PredicateMap predicateMap, RefObjectMap refObjectMap) {
        this(id, MappedTypes.PREDICATE_REF_OBJECT_MAP);
    }
}
