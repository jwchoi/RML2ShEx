package rml2shex.mapping.model.rml;

public class PredicateMap extends TermMap implements Comparable<PredicateMap> {

    @Override
    public int compareTo(PredicateMap predicateMap) {
        return getConstant().get().compareTo(predicateMap.getConstant().get());
    }
}
