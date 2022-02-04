package rml2shex.model.shex;

import rml2shex.commons.IRI;
import rml2shex.datasource.Column;
import rml2shex.model.rml.ObjectMap;
import rml2shex.model.rml.PredicateMap;
import rml2shex.commons.Symbols;
import rml2shex.model.rml.Template;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

public class TripleConstraint extends DeclarableTripleExpr {

    enum MappedTypes {CLASS, PREDICATE_OBJECT_MAP, PREDICATE_REF_OBJECT_MAP}

    private final MappedTypes mappedType;

    private Optional<Boolean> inverse;
    private IRI predicate;
    private Optional<ShapeExpr> valueExpr;
    private Optional<Integer> min; // if empty, 1
    private Optional<Integer> max; // if empty, 1. && -1 is treated as unbounded

    private TripleConstraint(MappedTypes mappedType, IRI id) {
        super(Kinds.TripleConstraint, id);
        this.mappedType = mappedType;

        inverse = Optional.empty();
        valueExpr = Optional.empty();
        min = Optional.empty();
        max = Optional.empty();
    }

    TripleConstraint(IRI predicate, Set<IRI> classes) { this(null, predicate, classes); }
    TripleConstraint(PredicateMap predicateMap, ObjectMap objectMap) { this(null, predicateMap, objectMap); }
    TripleConstraint(PredicateMap predicateMap, IRI referenceIdFromRefObjectMap, boolean inverse) { this(null, predicateMap, referenceIdFromRefObjectMap, inverse); }


    TripleConstraint(IRI id, IRI predicate, Set<IRI> classes) {
        this(MappedTypes.CLASS, id);
        convert(predicate, classes);
    }

    TripleConstraint(IRI id, PredicateMap predicateMap, ObjectMap objectMap) {
        this(MappedTypes.PREDICATE_OBJECT_MAP, id);
        convert(predicateMap, objectMap);
    }

    TripleConstraint(IRI id, PredicateMap predicateMap, IRI referenceIdFromRefObjectMap, boolean inverse) {
        this(MappedTypes.PREDICATE_REF_OBJECT_MAP, id);
        convert(predicateMap, referenceIdFromRefObjectMap, inverse);
    }

    private void convert(IRI predicate, Set<IRI> classes) {
        setInverse(false);

        setPredicate(predicate);

        IRI ncId = NodeConstraint.IdGenerator.generateId(getId().getPrefixLabel(), getId().getPrefixIRI(), "NC");
        ShapeExpr nc = new NodeConstraint(ncId, classes);
        setValueExpr(nc);

        int size = classes.size();
        setMin(size);
        setMax(size);
    }

    private void convert(PredicateMap predicateMap, IRI referenceIdFromRefObjectMap, boolean inverse) {
        setInverse(inverse);

        setPredicate(predicateMap);

        ShapeExpr sER = new ShapeExprRef(referenceIdFromRefObjectMap);
        setValueExpr(sER);

        setMin(0); // temporarily
        setMax(1); // temporarily
    }

    private void convert(PredicateMap predicateMap, ObjectMap objectMap) {
        setInverse(false);

        setPredicate(predicateMap);

        ShapeExpr nc = new NodeConstraint(objectMap);
        setValueExpr(nc);

        setMin(objectMap);
        setMax(-1); // temporarily
    }

    private void setMin(ObjectMap objectMap) {
        Optional<Column> reference = objectMap.getReference();
        if (reference.isPresent()) {
            if (reference.get().isIncludeNull()) setMin(0);
            else setMin(1);
        }

        Optional<Column> column = objectMap.getColumn();
        if (column.isPresent()) {
            if (column.get().isIncludeNull()) setMin(0);
            else setMin(1);
        }

        Optional<Template> template = objectMap.getTemplate();
        if (template.isPresent()) {
            if (template.get().getLogicalReferences().stream().filter(col -> col.isIncludeNull()).count() > 0) setMin(0);
            else setMin(1);
        }
    }

    private void setInverse(boolean inverse) { this.inverse = Optional.of(inverse); }

    private void setMin(int min) { if (min != 1) this.min = Optional.of(min); }
    private void setMax(int max) { if (max != 1) this.max = Optional.of(max); }

    private void setPredicate(IRI predicate) { this.predicate = predicate; }

    private void setPredicate(PredicateMap predicateMap) { setPredicate(predicateMap.getIRIConstant().get()); }

    private void setValueExpr(ShapeExpr shapeExpr) { valueExpr = Optional.ofNullable(shapeExpr); }

    @Override
    String getSerializedTripleExpr() {
        StringBuffer sb = new StringBuffer(super.getSerializedTripleExpr());

        // senseFlags?
        String senseFlags = inverse.isPresent() && inverse.get() ? Symbols.CARET : Symbols.EMPTY;
        sb.append(senseFlags);

        // predicate
        IRI rdfType = new IRI("rdf", URI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns#"), "type");
        String predicate = this.predicate.equals(rdfType) ? Symbols.A : this.predicate.getPrefixedNameOrElseAbsoluteIRI();
        sb.append(predicate + Symbols.SPACE);

        // inlineShapeExpression
        String inlineShapeExpression = valueExpr.isPresent() ? valueExpr.get().getSerializedShapeExpr() : Symbols.DOT;
        sb.append(inlineShapeExpression);

        // cardinality?
        String cardinality;
        int min = this.min.orElse(1);
        int max = this.max.orElse(1);

        if (min == 1 && max == 1) cardinality = Symbols.EMPTY; // the default of "exactly one"
        else {
            cardinality = Symbols.SPACE;

            if (min == 1 && max == -1) cardinality += Symbols.PLUS; // "+" - one or more
            else if (min == 0 && max == -1) cardinality += Symbols.ASTERISK; // "*" - zero or more
            else if (min == 0 && max == 1) cardinality += Symbols.QUESTION_MARK; // "?" - zero or one
            else if (min == max) cardinality += Symbols.OPEN_BRACE + min + Symbols.CLOSE_BRACE; // "{m}" - exactly m
            else cardinality = Symbols.OPEN_BRACE + min + Symbols.COMMA + (max != -1 ? max : Symbols.ASTERISK) + Symbols.CLOSE_BRACE; // "{m,n}" - at least m, no more than n
        }
        sb.append(cardinality);

        // annotation*

        // semanticActions

        return sb.toString();
    }
}