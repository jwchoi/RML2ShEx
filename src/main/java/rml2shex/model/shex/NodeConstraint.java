package rml2shex.model.shex;

import rml2shex.util.Symbols;
import rml2shex.util.Id;
import rml2shex.model.rml.SubjectMap;
import rml2shex.model.rml.Template;
import rml2shex.model.rml.TermMap;

import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class NodeConstraint extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

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

    private Optional<NodeKinds> nodeKind;
    private Optional<String> values;
    private Optional<String> datatype;
    private Set<XSFacet> xsFacets;

    NodeConstraint(Id id, SubjectMap subjectMap) {
        super(Kinds.NodeConstraint, id);

        nodeKind = Optional.empty();
        values = Optional.empty();
        datatype = Optional.empty();
        xsFacets = new HashSet<>();

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

    private void setNodeKind(NodeKinds nodeKind) { if (nodeKind != null) this.nodeKind = Optional.of(nodeKind); }

    private Optional<String> getValues() { return values; }
    private void setValues(Optional<String> values) { this.values = values; }

    private Optional<String> getDatatype() { return datatype; }
    private void setDatatype(Optional<String> datatype) { this.datatype = datatype; }

    private void addXsFacet(XSFacet xsFacet) { xsFacets.add(xsFacet); }

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

    @Override
    public String getSerializedShapeExpr() {
        String serializedShapeExpr = super.getSerializedShapeExpr();
        if (serializedShapeExpr != null) return serializedShapeExpr;

        StringBuffer sb = new StringBuffer();

        if (values.isPresent())
            sb.append(values.get());
        else if (datatype.isPresent())
            sb.append(datatype.get());
        else if (nodeKind.isPresent())
            sb.append(nodeKind.get());

        xsFacets.stream().forEach(xsFacet -> sb.append(Symbols.SPACE + xsFacet));

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}
