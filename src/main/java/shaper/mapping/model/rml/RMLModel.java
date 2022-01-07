package shaper.mapping.model.rml;

import java.net.URI;
import java.util.*;

public class RMLModel {
    private Map<String, String> prefixMap;
    private Set<TriplesMap> triplesMaps;
    private Set<Database> databases;
    private Set<Service> services;

    RMLModel() {
        prefixMap = new HashMap<>();
        triplesMaps = new HashSet<>();
    }

    public void addPrefixMap(String prefix, String uri) { prefixMap.put(prefix, uri); }

    public void addTriplesMap(TriplesMap triplesMap) { triplesMaps.add(triplesMap); }

    void setDatabases(Set<Database> databases) { this.databases = databases; }

    void setServices(Set<Service> services) { this.services = services; }

    public Set<TriplesMap> getTriplesMaps() { return triplesMaps; }

    public Optional<String> getRelativeIRI(URI iri) {
        Optional<String> relativeIRI = Optional.empty();

        for (String key : prefixMap.keySet()) {
            String value = prefixMap.get(key);
            String absoluteIRI = iri.toString();
            if (absoluteIRI.startsWith(value)) {
                relativeIRI = Optional.of(absoluteIRI.replace(value, key + ":"));
                break;
            }
        }

        return relativeIRI;
    }

    public Map<String, String> getPrefixMap() { return prefixMap; }
}
