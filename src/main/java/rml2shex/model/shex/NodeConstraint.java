package rml2shex.model.shex;

import rml2shex.util.Symbols;
import rml2shex.util.IRI;
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

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
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
    private Set<ValueSetValue> values;
    private Optional<IRI> datatype;
    private Set<XSFacet> xsFacets;

    private NodeConstraint(IRI id) {
        super(Kinds.NodeConstraint, id);

        nodeKind = Optional.empty();
        datatype = Optional.empty();
        xsFacets = new HashSet<>();
        values = new HashSet<>();
    }

    NodeConstraint(IRI id, SubjectMap subjectMap) {
        this(id);

        convert(subjectMap);
    }

    NodeConstraint(IRI id, Set<IRI> classes) {
        this(id);

        convert(classes);
    }

    private void convert(SubjectMap subjectMap) {
        setNodeKind(subjectMap);
        addXsFacet(subjectMap);
    }

    private void convert(Set<IRI> classes) {

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

    private Set<ValueSetValue> getValues() { return values; }
    private void setValues(Set<ValueSetValue> values) { this.values = values; }

    private Optional<IRI> getDatatype() { return datatype; }
    private void setDatatype(Optional<IRI> datatype) { this.datatype = datatype; }

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

        if (values.size() > 0)
            sb.append(values);
        else if (datatype.isPresent())
            sb.append(datatype.get().getPrefixedName());
        else if (nodeKind.isPresent())
            sb.append(nodeKind.get());

        xsFacets.stream().forEach(xsFacet -> sb.append(Symbols.SPACE + xsFacet));

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}