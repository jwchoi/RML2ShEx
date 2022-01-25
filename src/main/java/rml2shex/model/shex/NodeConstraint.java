package rml2shex.model.shex;

import rml2shex.model.rml.ObjectMap;
import rml2shex.util.Symbols;
import rml2shex.util.IRI;
import rml2shex.model.rml.SubjectMap;
import rml2shex.model.rml.Template;
import rml2shex.model.rml.TermMap;

import java.net.URI;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeConstraint extends DeclarableShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;

        private static int getPostfix() {
            return incrementer++;
        }

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
        public String toString() {
            return facet;
        }
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

    NodeConstraint(IRI id, ObjectMap objectMap) {
        this(id);
        convert(objectMap);
    }

    private void convert(SubjectMap subjectMap) {
        setNodeKind(subjectMap);
        setXsFacet(subjectMap);
    }

    private void convert(Set<IRI> classes) {
        setValues(classes);
    }

    private void convert(ObjectMap objectMap) {
        setNodeKind(objectMap);
        setValues(objectMap);
        setDatatype(objectMap);
        setXsFacet(objectMap);
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

    private void setNodeKind(ObjectMap objectMap) {
        Optional<TermMap.TermTypes> termType = objectMap.getTermType();

        if (termType.isPresent()) {
            if (termType.get().equals(TermMap.TermTypes.BLANKNODE)) {
                setNodeKind(NodeKinds.BNODE);
                return;
            }
            if (termType.get().equals(TermMap.TermTypes.IRI)) {
                setNodeKind(NodeKinds.IRI);
                return;
            }
            if (termType.get().equals(TermMap.TermTypes.LITERAL)) {
                setNodeKind(NodeKinds.LITERAL);
                return;
            }
        }
    }

    private void setNodeKind(NodeKinds nodeKind) {
        if (nodeKind != null) this.nodeKind = Optional.of(nodeKind);
    }

    private void setValues(Set<IRI> classes) { classes.stream().forEach(cls -> values.add(new ValueSetValue.ObjectValue.IRIREF(cls))); }

    private void setValues(ObjectMap objectMap) {
        if (objectMap.getLanguageMap().isPresent()) {
            Optional<String> languageTag = objectMap.getLanguageMap().get().getLiteralConstant();
            if (languageTag.isPresent()) values.add(new ValueSetValue.Language(languageTag.get()));
        }
    }

    private void setDatatype(ObjectMap objectMap) { datatype = objectMap.getDatatype(); }

    private void setXsFacet(TermMap termMap) {
        Optional<Template> template = termMap.getTemplate();
        if (template.isEmpty()) return;

        StringFacet stringFacet = new StringFacet(template.get());

        xsFacets.add(stringFacet);
    }

    private boolean isEquivalentXSFacet(Set<XSFacet> other) {
        for (XSFacet f1 : xsFacets) {
            boolean contains = false;
            for (XSFacet f2 : other) {
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

        if (values.size() > 0) {
            String valueSet = values.stream()
                    .map(ValueSetValue::getSerializedValueSetValue)
                    .sorted()
                    .collect(Collectors.joining(Symbols.SPACE, Symbols.OPEN_BRACKET, Symbols.CLOSE_BRACKET));

            sb.append(valueSet);
        } else if (datatype.isPresent())
            sb.append(datatype.get().getPrefixedName());
        else if (nodeKind.isPresent())
            sb.append(nodeKind.get());

        xsFacets.stream().forEach(xsFacet -> sb.append(Symbols.SPACE + xsFacet));

        serializedShapeExpr = sb.toString();
        setSerializedShapeExpr(serializedShapeExpr);
        return serializedShapeExpr;
    }
}