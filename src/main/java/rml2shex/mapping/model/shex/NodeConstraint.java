package rml2shex.mapping.model.shex;

import rml2shex.util.Symbols;
import rml2shex.util.ID;
import rml2shex.mapping.model.rml.SubjectMap;
import rml2shex.mapping.model.rml.Template;
import rml2shex.mapping.model.rml.TermMap;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class NodeConstraint extends ShapeExpr implements Comparable<NodeConstraint> {

    private static int incrementer = 0;

    static int getIncrementer() { return incrementer++; }

    protected enum XSFacets {
        MAX_LENGTH("MAXLENGTH"),
        MIN_INCLUSIVE("MININCLUSIVE"), MAX_INCLUSIVE("MAXINCLUSIVE"),
        TOTAL_DIGITS("TOTALDIGITS"), FRACTION_DIGITS("FRACTIONDIGITS");

        private final String facet;

        XSFacets(String facet) {
            this.facet = facet;
        }

        @Override
        public String toString() { return facet; }
    }

    private Optional<ID> id;

    private String serializedNodeConstraint;

    private Optional<NodeKinds> nodeKind;
    private Optional<String> values;
    private Optional<String> datatype;
    private Set<XSFacet> xsFacets;

    private NodeConstraint() {
        super(Kinds.NodeConstraint);

        nodeKind = Optional.empty();
        values = Optional.empty();
        datatype = Optional.empty();
        xsFacets = new HashSet<>();
    }

    NodeConstraint(ID id, SubjectMap subjectMap) {
        this();

        this.id = Optional.of(id);

        map(subjectMap);
    }

    private void map(SubjectMap subjectMap) {
        setNodeKind(subjectMap);
        addXsFacet(subjectMap);
    }

    private void setNodeKind(SubjectMap subjectMap) {
        Optional<TermMap.TermTypes> termType = subjectMap.getTermType();

        if (termType.isPresent()) {
            if (termType.get().equals(TermMap.TermTypes.BLANKNODE)) {
                setNodeKind(NodeKinds.BNODE);
                return;
            }
        }

        setNodeKind(NodeKinds.IRI);
    }

    private void addXsFacet(SubjectMap subjectMap) {
        Optional<Template> template = subjectMap.getTemplate();
        if (template.isEmpty()) return;

        StringFacet stringFacet = new StringFacet(template.get());

        addXsFacet(stringFacet);
    }

    ID getID() { return id.isPresent() ? id.get() : null; }

    private String getSerializedNodeConstraint() { return serializedNodeConstraint; }
    private void setSerializedNodeConstraint(String serializedNodeConstraint) {
        this.serializedNodeConstraint = serializedNodeConstraint;
    }

    private void setNodeKind(NodeKinds nodeKind) { if (nodeKind != null) this.nodeKind = Optional.of(nodeKind); }

    private Optional<String> getValues() { return values; }
    private void setValues(Optional<String> values) { this.values = values; }

    private Optional<String> getDatatype() { return datatype; }
    private void setDatatype(Optional<String> datatype) { this.datatype = datatype; }

    private void addXsFacet(XSFacet xsFacet) { xsFacets.add(xsFacet); }

    private void buildSerializedNodeConstraint() {
        StringBuffer nodeConstraint = new StringBuffer();

        if (values.isPresent())
            nodeConstraint.append(values.get());
        else if (datatype.isPresent())
            nodeConstraint.append(datatype.get());
        else if (nodeKind.isPresent())
            nodeConstraint.append(nodeKind.get());

        xsFacets.stream().forEach(xsFacet -> nodeConstraint.append(Symbols.SPACE + xsFacet));

        setSerializedNodeConstraint(nodeConstraint.toString());
    }

    @Override
    public int compareTo(NodeConstraint o) {
        return serializedNodeConstraint.compareTo(o.toString());
    }

    private boolean isEquivalentXSFacet(Set<XSFacet> other) {
        for (XSFacet f1: xsFacets) {
            boolean contains = false;
            for (XSFacet f2: other) {
                if (f1.isEquivalent(f2)) {
                    contains = true;
                    break;
                }
            }
            if (!contains) return false;
        }

        return true;
    }

    boolean isEquivalent(NodeConstraint other) {
        if (!nodeKind.equals(other.nodeKind)) return false;

        if (!isEquivalentXSFacet(other.xsFacets)) return false;

        return true;
    }
}
