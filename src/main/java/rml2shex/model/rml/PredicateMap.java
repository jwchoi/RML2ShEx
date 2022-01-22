package rml2shex.model.rml;

public class PredicateMap extends TermMap implements Comparable<PredicateMap> {

    @Override
    public int compareTo(PredicateMap predicateMap) {
        return getIRIConstant().get().compareTo(predicateMap.getIRIConstant().get());
    }
}
