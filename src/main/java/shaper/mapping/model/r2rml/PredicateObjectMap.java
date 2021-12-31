package shaper.mapping.model.r2rml;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PredicateObjectMap {
    private Set<PredicateMap> predicateMaps;
    private List<ObjectMap> objectMaps;
    private List<RefObjectMap> refObjectMaps;

    PredicateObjectMap() {
        objectMaps = new ArrayList<>();
        refObjectMaps = new ArrayList<>();
    }

    void setPredicateMaps(Set<PredicateMap> predicateMaps) { this.predicateMaps = predicateMaps; }

    void addObjectMap(ObjectMap objectMap) { objectMaps.add(objectMap); }
    void addRefObjectMap(RefObjectMap refObjectMap) { refObjectMaps.add(refObjectMap); }

    public List<PredicateObjectPair> getPredicateObjectPairs() {
        List<PredicateObjectPair> predicateObjectPairs = new ArrayList<>();

        for (PredicateMap predicateMap: predicateMaps) {
            for (ObjectMap objectMap: objectMaps)
                predicateObjectPairs.add(new PredicateObjectPair(predicateMap, objectMap));

            for (RefObjectMap refObjectMap: refObjectMaps)
                predicateObjectPairs.add(new PredicateObjectPair(predicateMap, refObjectMap));
        }

        return predicateObjectPairs;
    }

    public class PredicateObjectPair {
        private PredicateMap predicateMap;

        // either objectMap or refObjectMap
        private Optional<ObjectMap> objectMap;
        private Optional<RefObjectMap> refObjectMap;

        private PredicateObjectPair(PredicateMap predicateMap) { this.predicateMap = predicateMap; }

        PredicateObjectPair(PredicateMap predicateMap, ObjectMap objectMap) {
            this(predicateMap);
            this.objectMap = Optional.of(objectMap);
            refObjectMap = Optional.empty();
        }

        PredicateObjectPair(PredicateMap predicateMap, RefObjectMap refObjectMap) {
            this(predicateMap);
            this.refObjectMap = Optional.of(refObjectMap);
            objectMap = Optional.empty();
        }

        public PredicateMap getPredicateMap() { return predicateMap; }
        public Optional<ObjectMap> getObjectMap() { return objectMap; }
        public Optional<RefObjectMap> getRefObjectMap() { return refObjectMap; }
    }
}