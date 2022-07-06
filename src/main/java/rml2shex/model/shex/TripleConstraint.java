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
    private Optional<Boolean> inverse;
    private IRI predicate;
    private Optional<ShapeExpr> valueExpr;
    private Optional<Integer> min; // if empty, 0
    private Optional<Integer> max; // if empty, -1. && -1 is treated as unbounded

    private TripleConstraint(IRI id) {
        super(Kinds.TripleConstraint, id);

        inverse = Optional.empty();
        valueExpr = Optional.empty();
        min = Optional.empty();
        max = Optional.empty();
    }

    TripleConstraint(IRI predicate, Set<IRI> classes) { this(null, predicate, classes); }
    TripleConstraint(PredicateMap predicateMap, ObjectMap objectMap, Optional<Long> minOccurs, Optional<Long> maxOccurs) {
        this(null, predicateMap, objectMap, minOccurs, maxOccurs);
    }
    TripleConstraint(PredicateMap predicateMap, IRI referenceIdFromRefObjectMap, Optional<Long> minOccurs, Optional<Long> maxOccurs, boolean inverse) {
        this(null, predicateMap, referenceIdFromRefObjectMap, minOccurs, maxOccurs, inverse);
    }


    TripleConstraint(IRI id, IRI predicate, Set<IRI> classes) {
        this(id);
        convert(predicate, classes);
    }

    TripleConstraint(IRI id, PredicateMap predicateMap, ObjectMap objectMap, Optional<Long> minOccurs, Optional<Long> maxOccurs) {
        this(id);
        convert(predicateMap, objectMap, minOccurs, maxOccurs);
    }

    TripleConstraint(IRI id, PredicateMap predicateMap, IRI referenceIdFromRefObjectMap, Optional<Long> minOccurs, Optional<Long> maxOccurs, boolean inverse) {
        this(id);
        convert(predicateMap, referenceIdFromRefObjectMap, minOccurs, maxOccurs, inverse);
    }

    private void convert(IRI predicate, Set<IRI> classes) {
        setInverse(false);

        setPredicate(predicate);

        ShapeExpr nc = new NodeConstraint(classes);
        setValueExpr(nc);

        int size = classes.size();
        setMin(size);
        setMax(size);
    }

    private void convert(PredicateMap predicateMap, IRI referenceIdFromRefObjectMap, Optional<Long> minOccurs, Optional<Long> maxOccurs, boolean inverse) {
        setInverse(inverse);

        setPredicate(predicateMap);

        ShapeExpr sER = new ShapeExprRef(referenceIdFromRefObjectMap);
        setValueExpr(sER);

        if (minOccurs.isPresent() && minOccurs.get() > 0) setMin(1);
        if (maxOccurs.isPresent()) {
            if (maxOccurs.get() == 0) setMax(0);
            else if (maxOccurs.get() == 1) setMax(1);
        }
    }

    private void convert(PredicateMap predicateMap, ObjectMap objectMap, Optional<Long> minOccurs, Optional<Long> maxOccurs) {
        setInverse(false);

        setPredicate(predicateMap);

        ShapeExpr nc = new NodeConstraint(objectMap);
        setValueExpr(nc);

        setMin(objectMap, minOccurs);
        setMax(objectMap, maxOccurs);
    }

    private void setMin(ObjectMap objectMap, Optional<Long> minOccurs) {
        Optional<IRI> iriConstant = objectMap.getIRIConstant();
        if (iriConstant.isPresent()) setMin(1);

        Optional<String> literalConstant = objectMap.getLiteralConstant();
        if (literalConstant.isPresent()) setMin(1);

        // when column, reference, template
        if (minOccurs.isPresent() && minOccurs.get() == 1) setMin(1);
    }

    private void setMax(ObjectMap objectMap, Optional<Long> maxOccurs) {
        Optional<IRI> iriConstant = objectMap.getIRIConstant();
        if (iriConstant.isPresent()) setMax(1);

        Optional<String> literalConstant = objectMap.getLiteralConstant();
        if (literalConstant.isPresent()) setMax(1);

        // when column, reference, template
        if (maxOccurs.isPresent()) {
            if (maxOccurs.get() == 0) setMax(0);
            else if (maxOccurs.get() == 1) setMax(1);
        }
    }

    private void setInverse(boolean inverse) { this.inverse = Optional.of(inverse); }

    private void setMin(int min) { if (min != 0) this.min = Optional.of(min); }
    private void setMax(int max) { if (max != -1) this.max = Optional.of(max); }

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
        int min = this.min.orElse(0);
        int max = this.max.orElse(-1);

        if (min == 1 && max == 1) cardinality = Symbols.EMPTY; // the default of "exactly one"
        else {
            cardinality = Symbols.SPACE;

            if (min == 1 && max == -1) cardinality += Symbols.PLUS; // "+" - one or more
            else if (min == 0 && max == -1) cardinality += Symbols.ASTERISK; // "*" - zero or more
            else if (min == 0 && max == 1) cardinality += Symbols.QUESTION_MARK; // "?" - zero or one
            else if (min == max) cardinality += Symbols.OPEN_BRACE + min + Symbols.CLOSE_BRACE; // "{m}" - exactly m
            else cardinality += Symbols.OPEN_BRACE + min + Symbols.COMMA + (max != -1 ? max : Symbols.ASTERISK) + Symbols.CLOSE_BRACE; // "{m,n}" - at least m, no more than n
        }
        sb.append(cardinality);

        // annotation*

        // semanticActions

        return sb.toString();
    }
}