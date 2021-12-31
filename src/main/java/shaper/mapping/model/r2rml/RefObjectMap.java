package shaper.mapping.model.r2rml;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RefObjectMap implements Comparable<RefObjectMap> {

    private URI parentTriplesMap;
    private List<JoinCondition> joinConditions;

    RefObjectMap(URI parentTriplesMap) {
        this.parentTriplesMap = parentTriplesMap;
        joinConditions = new ArrayList<>();
    }

    public void addJoinCondition(String child, String parent) { joinConditions.add(new JoinCondition(child, parent)); }

    public URI getParentTriplesMap() { return parentTriplesMap; }

    @Override
    public int compareTo(RefObjectMap refObjectMap) {
        return parentTriplesMap.compareTo(refObjectMap.parentTriplesMap);
    }
}
