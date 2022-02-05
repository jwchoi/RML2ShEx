package rml2shex.model.rml;

import rml2shex.datasource.Column;

public class JoinCondition {
    private Column child;
    private Column parent;

    JoinCondition(String child, String parent) {
        this.child = new Column(child);
        this.parent = new Column(parent);
    }

    public Column getChild() { return child; }

    public Column getParent() { return parent; }
}
