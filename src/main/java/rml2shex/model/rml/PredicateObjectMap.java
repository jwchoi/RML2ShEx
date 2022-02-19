package rml2shex.model.rml;

import java.util.*;

public class PredicateObjectMap {
    private Set<PredicateMap> predicateMaps;
    private List<ObjectMap> objectMaps;
    private List<RefObjectMap> refObjectMaps;

    private Set<GraphMap> graphMaps; // the size of graphMaps >= 0

    private List<PredicateObjectPair> predicateObjectPairs;

    PredicateObjectMap() {
        predicateMaps = new TreeSet<>();
        objectMaps = new ArrayList<>();
        refObjectMaps = new ArrayList<>();
    }

    void addPredicateMap(PredicateMap predicateMap) { predicateMaps.add(predicateMap); }

    void addObjectMap(ObjectMap objectMap) { objectMaps.add(objectMap); }
    void addRefObjectMap(RefObjectMap refObjectMap) { refObjectMaps.add(refObjectMap); }

    void setGraphMaps(Set<GraphMap> graphMaps) { this.graphMaps = graphMaps; }

    public List<PredicateObjectPair> getPredicateObjectPairs() {
        if (predicateObjectPairs != null) return predicateObjectPairs;

        predicateObjectPairs = new ArrayList<>();

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

        private Optional<Long> minOccurs; // acquired from the data source
        private Optional<Long> inverseMinOccurs; // acquired from the data source
        private Optional<Long> maxOccurs; // acquired from the data source
        private Optional<Long> inverseMaxOccurs; // acquired from the data source

        private PredicateObjectPair(PredicateMap predicateMap) {
            this.predicateMap = predicateMap;
            minOccurs = Optional.empty();
            inverseMinOccurs = Optional.empty();
            maxOccurs = Optional.empty();
            inverseMaxOccurs = Optional.empty();
        }

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

        public Optional<Long> getMinOccurs() { return minOccurs; }
        public void setMinOccurs(long minOccurs) { this.minOccurs = Optional.of(minOccurs); }

        public Optional<Long> getInverseMinOccurs() { return inverseMinOccurs; }
        public void setInverseMinOccurs(long inverseMinOccurs) { this.inverseMinOccurs = Optional.of(inverseMinOccurs); }

        public Optional<Long> getMaxOccurs() { return maxOccurs; }
        public void setMaxOccurs(long maxOccurs) { this.maxOccurs = Optional.of(maxOccurs); }

        public Optional<Long> getInverseMaxOccurs() { return inverseMaxOccurs; }
        public void setInverseMaxOccurs(long inverseMaxOccurs) { this.inverseMaxOccurs = Optional.of(inverseMaxOccurs); }
    }
}