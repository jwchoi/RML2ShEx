package rml2shex.model.rml;

import java.util.*;

public class RMLModel {
    private Map<String, String> prefixMap;
    private Set<TriplesMap> triplesMaps;

    RMLModel() {
        prefixMap = new HashMap<>();
        triplesMaps = new HashSet<>();
    }

    public void addPrefixMap(String prefix, String uri) { prefixMap.put(prefix, uri); }

    public void addTriplesMap(TriplesMap triplesMap) { triplesMaps.add(triplesMap); }

    public Set<TriplesMap> getTriplesMaps() { return triplesMaps; }

    public Map<String, String> getPrefixMap() { return prefixMap; }
}
