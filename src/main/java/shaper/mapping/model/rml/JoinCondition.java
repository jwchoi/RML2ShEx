package shaper.mapping.model.rml;

public class JoinCondition {
    private String child;
    private String parent;

    JoinCondition(String child, String parent) {
        this.child = child;
        this.parent = parent;
    }
}
