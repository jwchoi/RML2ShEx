package shaper.mapping.model.rml;

import java.net.URI;

public class PredicateMap extends TermMap implements Comparable<PredicateMap> {

    PredicateMap(URI predicate) {
        setConstant(predicate.toString());
        setTermType(TermTypes.IRI);
    }

    @Override
    public int compareTo(PredicateMap predicateMap) {
        return getConstant().get().compareTo(predicateMap.getConstant().get());
    }
}
