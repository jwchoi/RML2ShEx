package shaper.mapping.model.shex;

import janus.database.SQLSelectField;
import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.r2rml.ObjectMap;
import shaper.mapping.model.r2rml.PredicateMap;
import shaper.mapping.model.r2rml.RefObjectMap;
import shaper.mapping.model.r2rml.Template;

import java.net.URI;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class R2RMLTripleConstraint extends TripleConstraint {
    private Set<URI> classIRIs;
    private PredicateMap predicateMap;
    private ObjectMap objectMap;
    private RefObjectMap refObjectMap;

    R2RMLTripleConstraint(Set<URI> classIRIs) {
        super(MappedTypes.RR_CLASSES);
        this.classIRIs = classIRIs;
    }

    R2RMLTripleConstraint(PredicateMap predicateMap, ObjectMap objectMap) {
        super(MappedTypes.PREDICATE_OBJECT_MAP);
        this.predicateMap = predicateMap;
        this.objectMap = objectMap;
    }

    R2RMLTripleConstraint(PredicateMap predicateMap, RefObjectMap refObjectMap) {
        super(MappedTypes.REF_OBJECT_MAP);
        this.predicateMap = predicateMap;
        this.refObjectMap = refObjectMap;
    }

    Set<URI> getClassIRIs() { return classIRIs; }

    Optional<RefObjectMap> getRefObjectMap() { return Optional.ofNullable(refObjectMap); }

    private String buildTripleConstraint() {
        String tripleConstraint = null;
        switch (getMappedType()) {
            case RR_CLASSES:
                tripleConstraint = buildTripleConstraintFromClasses();
                break;
            case PREDICATE_OBJECT_MAP:
                tripleConstraint = buildTripleConstraintFromPredicateObjectMap();
                break;
            case REF_OBJECT_MAP:
                tripleConstraint = buildTripleConstraintFromRefObjectMap();
                break;
        }
        return tripleConstraint;
    }

    private String buildTripleConstraintFromRefObjectMap() {
        // property
        String property = buildProperty(predicateMap);

        // shapeRef
        String prefix = Shaper.shexMapper.shExSchema.getPrefix();
        String shapeRef = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedShape(refObjectMap.getParentTriplesMap()).getShapeID();

        // cardinality
        setCardinality(Symbols.ASTERISK);

        return property + Symbols.SPACE + Symbols.AT + shapeRef + Symbols.SPACE + getCardinality();
    }

    private String buildProperty(PredicateMap predicateMap) {
        String property = predicateMap.getConstant().get();
        Optional<String> relativeIRI = Shaper.shexMapper.r2rmlModel.getRelativeIRI(URI.create(property));
        if (relativeIRI.isPresent())
            property = relativeIRI.get();
        else
            property = Symbols.LT + property + Symbols.GT;

        return property;
    }

    private String buildTripleConstraintFromClasses() {
        String rdfType = Symbols.A;

        StringBuffer classes = new StringBuffer(Symbols.SPACE);
        for (URI classIRI: classIRIs) {
            Optional<String> relativeIRI = Shaper.shexMapper.r2rmlModel.getRelativeIRI(classIRI);
            String clsIRI = relativeIRI.isPresent() ? relativeIRI.get() : classIRI.toString();

            classes.append(clsIRI + Symbols.SPACE);
        }

        // cardinality
        int sizeOfClassIRIs = classIRIs.size();
        if (sizeOfClassIRIs == 1) setCardinality("");
        else setCardinality(Symbols.OPEN_BRACE + sizeOfClassIRIs + Symbols.CLOSE_BRACE);

        return rdfType + Symbols.SPACE + Symbols.OPEN_BRACKET + classes + Symbols.CLOSE_BRACKET + Symbols.SPACE + getCardinality();
    }

    private String buildTripleConstraintFromPredicateObjectMap() {
        // property
        String property = buildProperty(predicateMap);

        // cardinality
        Optional<SQLSelectField> sqlSelectField = objectMap.getColumn();
        if (sqlSelectField.isPresent()) {
            switch (sqlSelectField.get().getNullable()) {
                case ResultSetMetaData.columnNoNulls:
                    setCardinality(""); // the default of "exactly one"
                    break;
                case ResultSetMetaData.columnNullable:
                case ResultSetMetaData.columnNullableUnknown:
                    setCardinality(Symbols.QUESTION_MARK); // "?" - zero or one
            }
        } else {
            Optional<Template> template = objectMap.getTemplate();
            if (template.isPresent()) {
                List<SQLSelectField> columnNames = template.get().getColumnNames();
                setCardinality(""); // the default of "exactly one"
                for (SQLSelectField columnName: columnNames) {
                    if (columnName.getNullable() != ResultSetMetaData.columnNoNulls) {
                        setCardinality(Symbols.QUESTION_MARK); // "?" - zero or one
                        break;
                    }
                }
            }
        }

        if (R2RMLNodeConstraint.isPossibleToHaveXSFacet(objectMap)) {
            String prefix = Shaper.shexMapper.shExSchema.getPrefix();
            return property + Symbols.SPACE + Symbols.AT + prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedNodeConstraintID(objectMap) + Symbols.SPACE + getCardinality();
        } else
            return property + Symbols.SPACE + Shaper.shexMapper.shExSchema.getMappedNodeConstraint(objectMap) + Symbols.SPACE + getCardinality();
    }

    @Override
    public String toString() {
        if (getSerializedTripleConstraint() == null)
            setSerializedTripleConstraint(buildTripleConstraint());

        return getSerializedTripleConstraint();
    }
}
