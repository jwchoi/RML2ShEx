package rml2shex.model.shex;

import rml2shex.datasource.Column;
import rml2shex.model.rml.ObjectMap;
import rml2shex.commons.Symbols;
import rml2shex.commons.IRI;
import rml2shex.model.rml.SubjectMap;
import rml2shex.model.rml.Template;
import rml2shex.model.rml.TermMap;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeConstraint extends DeclarableShapeExpr {

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

    NodeConstraint(SubjectMap subjectMap) { this(null, subjectMap); }
    NodeConstraint(Set<IRI> classes) { this(null, classes); }
    NodeConstraint(ObjectMap objectMap) { this(null, objectMap); }

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
        setValues(subjectMap);
        setXsFacet(subjectMap); // must call the last
    }

    private void convert(Set<IRI> classes) {
        setValues(classes);
    }

    private void convert(ObjectMap objectMap) {
        setNodeKind(objectMap);
        setValues(objectMap);
        setDatatype(objectMap);
        setXsFacet(objectMap); // must call the last
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

    private void setValues(SubjectMap subjectMap) {
        if (subjectMap.getIRIConstant().isPresent()) {
            IRI iriConstant = subjectMap.getIRIConstant().get();
            values.add(new ValueSetValue.ObjectValue.IRIREF(iriConstant));
        }
    }

    private void setValues(Set<IRI> classes) { classes.stream().forEach(cls -> values.add(new ValueSetValue.ObjectValue.IRIREF(cls))); }

    private void setValues(ObjectMap objectMap) {
        if (objectMap.getLiteralConstant().isPresent()) {
            String literalConstant = objectMap.getLiteralConstant().get();
            values.add(new ValueSetValue.ObjectValue.ObjectLiteral(literalConstant));
        } else if (objectMap.getIRIConstant().isPresent()) {
            IRI iriConstant = objectMap.getIRIConstant().get();
            values.add(new ValueSetValue.ObjectValue.IRIREF(iriConstant));
        } else if (objectMap.getLanguageMap().isPresent()) {
            Optional<String> languageTag = objectMap.getLanguageMap().get().getLiteralConstant();
            if (languageTag.isPresent()) values.add(new ValueSetValue.Language(languageTag.get()));
        }
    }

    private void setDatatype(ObjectMap objectMap) { datatype = objectMap.getDatatype(); }

    private void setXsFacet(SubjectMap subjectMap) {
        Optional<Template> template = subjectMap.getTemplate();
        if (template.isPresent()) {
            StringFacet stringFacet = new StringFacet(template.get());

            xsFacets.add(stringFacet);
        }
    }

    private void setXsFacet(ObjectMap objectMap) {
        Optional<Template> template = objectMap.getTemplate();
        if (template.isPresent()) {
            StringFacet stringFacet = new StringFacet(template.get());

            xsFacets.add(stringFacet);
        }

        Optional<Column> optionalColumn = objectMap.getColumn();
        if (optionalColumn.isPresent()) {
            Column column = optionalColumn.get();
            if (column.getMinValue().isPresent()) {
                NumericFacet numericFacet = new NumericFacet(NumericFacet.NumericRange.MIN_INCLUSIVE, column.getMinValue().get());
                xsFacets.add(numericFacet);
            }

            if (column.getMaxValue().isPresent()) {
                NumericFacet numericFacet = new NumericFacet(NumericFacet.NumericRange.MAX_INCLUSIVE, column.getMaxValue().get());
                xsFacets.add(numericFacet);
            }

            Optional<Integer> minLength = column.getMinLength();
            Optional<Integer> maxLength = column.getMaxLength();

            if (minLength.isPresent() && maxLength.isPresent() && minLength.get().equals(maxLength.get())) {
                StringFacet stringFacet = new StringFacet(StringFacet.StringLength.LENGTH, minLength.get());
                xsFacets.add(stringFacet);
            } else {
                if (minLength.isPresent()) {
                    StringFacet stringFacet = new StringFacet(StringFacet.StringLength.MIN_LENGTH, minLength.get());
                    xsFacets.add(stringFacet);
                }
                if (maxLength.isPresent()) {
                    StringFacet stringFacet = new StringFacet(StringFacet.StringLength.MAX_LENGTH, maxLength.get());
                    xsFacets.add(stringFacet);
                }
            }
        }

        Optional<Column> optionalReference = objectMap.getReference();
        if (optionalReference.isPresent()) {
            Column reference = optionalReference.get();



            if (reference.getMinValue().isPresent()) {
                NumericFacet numericFacet = new NumericFacet(NumericFacet.NumericRange.MIN_INCLUSIVE, reference.getMinValue().get());
                xsFacets.add(numericFacet);
            }

            if (reference.getMaxValue().isPresent()) {
                NumericFacet numericFacet = new NumericFacet(NumericFacet.NumericRange.MAX_INCLUSIVE, reference.getMaxValue().get());
                xsFacets.add(numericFacet);
            }

            Optional<Integer> minLength = reference.getMinLength();
            Optional<Integer> maxLength = reference.getMaxLength();

            if (minLength.isPresent() && maxLength.isPresent() && minLength.get().equals(maxLength.get())) {
                StringFacet stringFacet = new StringFacet(StringFacet.StringLength.LENGTH, minLength.get());
                xsFacets.add(stringFacet);
            } else {
                if (minLength.isPresent()) {
                    StringFacet stringFacet = new StringFacet(StringFacet.StringLength.MIN_LENGTH, minLength.get());
                    xsFacets.add(stringFacet);
                }
                if (maxLength.isPresent()) {
                    StringFacet stringFacet = new StringFacet(StringFacet.StringLength.MAX_LENGTH, maxLength.get());
                    xsFacets.add(stringFacet);
                }
            }
        }
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
        StringBuffer sb = new StringBuffer(super.getSerializedShapeExpr());

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

        return sb.toString();
    }
}