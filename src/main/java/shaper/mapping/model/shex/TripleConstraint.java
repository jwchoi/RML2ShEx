package shaper.mapping.model.shex;

import janus.database.DBColumn;
import janus.database.DBRefConstraint;
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

public class TripleConstraint implements Comparable<TripleConstraint> {

    enum MappedTypes { COLUMN, REF_CONSTRAINT, TABLE, RR_CLASSES, PREDICATE_OBJECT_MAP, REF_OBJECT_MAP }

    private String tripleConstraint;

    private MappedTypes mappedType;

    private String mappedTable;
    private DBColumn mappedColumn;
    private DBRefConstraint mappedRefConstraint;

    private Optional<Boolean> isInverse = Optional.empty();

    TripleConstraint(String mappedTable) {
        this.mappedTable = mappedTable;
        mappedType = MappedTypes.TABLE;
    }

    TripleConstraint(DBColumn mappedColumn) {
        this.mappedColumn = mappedColumn;
        mappedType = MappedTypes.COLUMN;
    }

    TripleConstraint(DBRefConstraint mappedRefConstraint, boolean isInverse) {
        this.mappedRefConstraint = mappedRefConstraint;
        mappedType = MappedTypes.REF_CONSTRAINT;
        this.isInverse = Optional.of(isInverse);
    }

    Optional<Boolean> isInverse() {
        return isInverse;
    }

    @Override
    public String toString() {
        if (tripleConstraint == null)
         tripleConstraint = buildTripleConstraint();

        return tripleConstraint;
    }

    private String buildTripleConstraint() {
        String tripleConstraint = null;
        switch (mappedType) {
            case COLUMN:
                tripleConstraint = buildTripleConstraintFromColumn();
                break;
            case REF_CONSTRAINT:
                tripleConstraint = isInverse.get() ? buildInverseTripleConstraintFromRefConstraint() : buildTripleConstraintFromRefConstraint();
                break;
            case TABLE:
                tripleConstraint = buildTripleConstraintFromTable();
                break;
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

    private String buildInverseTripleConstraintFromRefConstraint() {
        String table = mappedRefConstraint.getTableName();
        String refConstraint = mappedRefConstraint.getRefConstraintName();
        String refProperty = Symbols.LT + Shaper.shexMapper.dmModel.getMappedReferenceProperty(table, refConstraint) + Symbols.GT;

        String prefix = Shaper.shexMapper.shExSchema.getPrefix();
        String referencedShape = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedShapeID(table);

        cardinality = Symbols.ASTERISK;

        return Symbols.CARET + refProperty + Symbols.SPACE + Symbols.AT + referencedShape + cardinality;
    }

    private String buildTripleConstraintFromRefConstraint() {
        String table = mappedRefConstraint.getTableName();
        String refConstraint = mappedRefConstraint.getRefConstraintName();
        String refProperty = Symbols.LT + Shaper.shexMapper.dmModel.getMappedReferenceProperty(table, refConstraint) + Symbols.GT;

        String referencedTable = Shaper.dbSchema.getReferencedTableBy(table, refConstraint);
        String prefix = Shaper.shexMapper.shExSchema.getPrefix();
        String referencedShape = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedShapeID(referencedTable);

        List<String> columns = Shaper.dbSchema.getReferencingColumnsByOrdinalPosition(table, refConstraint);
        boolean nullable = false;
        for (String column: columns) {
            if (!Shaper.dbSchema.isNotNull(mappedTable, column)) {
                nullable = true;
                break;
            }
        }
        cardinality = nullable ? "?" : "";

        return refProperty + Symbols.SPACE + Symbols.AT + referencedShape + cardinality;
    }

    private String buildTripleConstraintFromColumn() {
        String table = mappedColumn.getTableName();
        String column = mappedColumn.getColumnName();
        String litProperty = Symbols.LT + Shaper.shexMapper.dmModel.getMappedLiteralProperty(table, column) + Symbols.GT;

        String prefix = Shaper.shexMapper.shExSchema.getPrefix();
        String nodeConstraint = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedNodeConstraintID(table, column);

        boolean nullable = false;
        if (!Shaper.dbSchema.isNotNull(table, column))
            nullable = true;

        cardinality = nullable ? "?" : "";

        return litProperty + Symbols.SPACE + Symbols.AT + nodeConstraint + cardinality;
    }

    private String buildTripleConstraintFromTable() {
        String rdfType = Symbols.A;

        String tableIRI = Symbols.LT + Shaper.shexMapper.dmModel.getMappedTableIRILocalPart(mappedTable) + Symbols.GT;

        cardinality = "";

        return rdfType + Symbols.SPACE + Symbols.OPEN_BRACKET + tableIRI + Symbols.CLOSE_BRACKET + cardinality;
    }

    @Override
    public int compareTo(TripleConstraint o) {
        return toString().compareTo(o.toString());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Set<URI> classIRIs;
    private PredicateMap predicateMap;
    private ObjectMap objectMap;
    private RefObjectMap refObjectMap;

    private String cardinality;

    TripleConstraint(Set<URI> classIRIs) {
        this.classIRIs = classIRIs;
        mappedType = MappedTypes.RR_CLASSES;
    }

    TripleConstraint(PredicateMap predicateMap, ObjectMap objectMap) {
        this.predicateMap = predicateMap;
        this.objectMap = objectMap;
        mappedType = MappedTypes.PREDICATE_OBJECT_MAP;
    }

    TripleConstraint(PredicateMap predicateMap, RefObjectMap refObjectMap) {
        this.predicateMap = predicateMap;
        this.refObjectMap = refObjectMap;
        mappedType = MappedTypes.REF_OBJECT_MAP;
    }

    Optional<RefObjectMap> getRefObjectMap() { return Optional.ofNullable(refObjectMap); }

    private String buildTripleConstraintFromRefObjectMap() {
        // property
        String property = buildProperty(predicateMap);

        // shapeRef
        String prefix = Shaper.shexMapper.shExSchema.getPrefix();
        String shapeRef = prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedShape(refObjectMap.getParentTriplesMap()).getShapeID();

        // cardinality
        cardinality = Symbols.ASTERISK;

        return property + Symbols.SPACE + Symbols.AT + shapeRef + Symbols.SPACE + cardinality;
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
        if (sizeOfClassIRIs == 1) cardinality = "";
        else cardinality = Symbols.OPEN_BRACE + sizeOfClassIRIs + Symbols.CLOSE_BRACE;

        return rdfType + Symbols.SPACE + Symbols.OPEN_BRACKET + classes + Symbols.CLOSE_BRACKET + Symbols.SPACE + cardinality;
    }

    private String buildTripleConstraintFromPredicateObjectMap() {
        // property
        String property = buildProperty(predicateMap);

        // cardinality
        Optional<SQLSelectField> sqlSelectField = objectMap.getColumn();
        if (sqlSelectField.isPresent()) {
            switch (sqlSelectField.get().getNullable()) {
                case ResultSetMetaData.columnNoNulls:
                    cardinality = ""; // the default of "exactly one"
                    break;
                case ResultSetMetaData.columnNullable:
                case ResultSetMetaData.columnNullableUnknown:
                    cardinality = Symbols.QUESTION_MARK; // "?" - zero or one
            }
        } else {
            Optional<Template> template = objectMap.getTemplate();
            if (template.isPresent()) {
                List<SQLSelectField> columnNames = template.get().getColumnNames();
                cardinality = ""; // the default of "exactly one"
                for (SQLSelectField columnName: columnNames) {
                    if (columnName.getNullable() != ResultSetMetaData.columnNoNulls) {
                        cardinality = Symbols.QUESTION_MARK; // "?" - zero or one
                        break;
                    }
                }
            }
        }

        if (NodeConstraint.isPossibleToHaveXSFacet(objectMap)) {
            String prefix = Shaper.shexMapper.shExSchema.getPrefix();
            return property + Symbols.SPACE + Symbols.AT + prefix + Symbols.COLON + Shaper.shexMapper.shExSchema.getMappedNodeConstraintID(objectMap) + Symbols.SPACE + cardinality;
        } else
            return property + Symbols.SPACE + Shaper.shexMapper.shExSchema.getMappedNodeConstraint(objectMap) + Symbols.SPACE + cardinality;
    }

    MappedTypes getMappedType() { return mappedType; }

    Set<URI> getClassIRIs() { return classIRIs; }
}