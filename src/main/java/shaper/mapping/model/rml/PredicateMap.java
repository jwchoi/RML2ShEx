package shaper.mapping.model.rml;

public class PredicateMap extends TermMap implements Comparable<PredicateMap> {
    PredicateMap(String predicate) {
        setConstant(predicate);
    }

    @Override
    public int compareTo(PredicateMap predicateMap) {
        return getConstant().get().compareTo(predicateMap.getConstant().get());
    }
}
