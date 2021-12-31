package shaper.mapping.model.shacl;

import janus.database.SQLSelectField;
import shaper.Shaper;
import shaper.mapping.SqlXsdMap;
import shaper.mapping.Symbols;
import shaper.mapping.XSDs;
import shaper.mapping.model.dm.LiteralProperty;
import shaper.mapping.model.dm.ReferenceProperty;
import shaper.mapping.model.dm.TableIRI;
import shaper.mapping.model.r2rml.*;

import java.net.URI;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PropertyShape extends Shape {
    private Optional<LiteralProperty> mappedLiteralProperty = Optional.empty();
    private Optional<ReferenceProperty> mappedReferenceProperty = Optional.empty();
    private boolean isInverse;

    PropertyShape(URI id, LiteralProperty mappedLiteralProperty, ShaclDocModel shaclDocModel) {
        super(id, shaclDocModel);
        this.mappedLiteralProperty = Optional.of(mappedLiteralProperty);

        mappingType = MappingTypes.LITERAL_PROPERTY;
    }

    PropertyShape(URI id, ReferenceProperty mappedReferenceProperty, boolean isInverse, ShaclDocModel shaclDocModel) {
        super(id, shaclDocModel);
        this.mappedReferenceProperty = Optional.of(mappedReferenceProperty);
        this.isInverse = isInverse;

        mappingType = MappingTypes.REFERENCE_PROPERTY;
    }

    private String buildSerializedPropertyShape(LiteralProperty literalProperty) {
        StringBuffer buffer = new StringBuffer();

        String o = null; // to be used as objects of different RDF triples

        // sh:nodeKind
        o = "sh:Literal";
        buffer.append(getPO("sh:nodeKind", o));
        buffer.append(getSNT());

        buffer.append(buildConstraints(literalProperty));

        return buffer.toString();
    }

    private String buildConstraints(LiteralProperty literalProperty) {
        StringBuffer buffer = new StringBuffer();

        String o = null; // to be used as objects of different RDF triples

        String mappedTable = literalProperty.getMappedTable();
        String mappedColumn = literalProperty.getMappedColumn();

        int JDBCDataType = Shaper.dbSchema.getJDBCDataType(mappedTable, mappedColumn);
        XSDs xsd = SqlXsdMap.getMappedXSD(JDBCDataType);

        // sh:datatype
        String datatype = xsd.getRelativeIRI();
        o = datatype;
        buffer.append(getPO("sh:datatype", o));
        buffer.append(getSNT());

        // sh:in
        Optional<Set<String>> valueSet = Shaper.dbSchema.getValueSet(mappedTable, mappedColumn);
        if (valueSet.isPresent()) {
            o = Symbols.OPEN_PARENTHESIS + Symbols.SPACE;
            Set<String> set = valueSet.get();
            for (String value: set) {
                if (value.startsWith(Symbols.SINGLE_QUOTATION_MARK) && value.endsWith(Symbols.SINGLE_QUOTATION_MARK)) {
                    value = value.substring(1, value.length()-1);
                }
                o += "\"" + value + "\"" + "^^" + datatype + Symbols.SPACE;
            }
            o += Symbols.CLOSE_PARENTHESIS;
            buffer.append(getPO("sh:in", o));
            buffer.append(getSNT());
        }

        switch (xsd) {
            case XSD_BOOLEAN:
                // sh:in
                o =  "\"true\"^^xsd:boolean" + Symbols.SPACE + "\"false\"^^xsd:boolean";
                o = Symbols.OPEN_PARENTHESIS + Symbols.SPACE + o + Symbols.SPACE + Symbols.CLOSE_PARENTHESIS;
                buffer.append(getPO("sh:in", o));
                buffer.append(getSNT());
                break;
            case XSD_DATE:
                // sh:pattern
                o =  Shaper.dbSchema.getRegexForXSDDate().replace("\\", "\\\\");
                o = Symbols.DOUBLE_QUOTATION_MARK + o + Symbols.DOUBLE_QUOTATION_MARK;
                buffer.append(getPO("sh:pattern", o));
                buffer.append(getSNT());

                // sh:minInclusive & sh:maxInclusive
                Optional<String> minimumDateValue = Shaper.dbSchema.getMinimumDateValue();
                if (minimumDateValue.isPresent()) {
                    o = "\"" + minimumDateValue.get() + "\"" + "^^xsd:date";
                    buffer.append(getPO("sh:minInclusive", o));
                    buffer.append(getSNT());
                }
                Optional<String> maximumDateValue = Shaper.dbSchema.getMaximumDateValue();
                if (maximumDateValue.isPresent()) {
                    o = "\"" + maximumDateValue.get() + "\"" + "^^xsd:date";
                    buffer.append(getPO("sh:maxInclusive", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_DATE_TIME:
                // sh:pattern
                Optional<String> dateTimeRegex = Shaper.dbSchema.getRegexForXSDDateTime(mappedTable, mappedColumn);
                if (dateTimeRegex.isPresent()) {
                    o = dateTimeRegex.get().replace("\\", "\\\\");
                    o = Symbols.DOUBLE_QUOTATION_MARK + o + Symbols.DOUBLE_QUOTATION_MARK;
                    buffer.append(getPO("sh:pattern", o));
                    buffer.append(getSNT());
                }

                // sh:minInclusive & sh:maxInclusive
                Optional<String> minimumDateTimeValue = Shaper.dbSchema.getMinimumDateTimeValue(mappedTable, mappedColumn);
                if (minimumDateTimeValue.isPresent()) {
                    o = "\"" + minimumDateTimeValue.get() + "\"" + "^^xsd:dateTime";
                    buffer.append(getPO("sh:minInclusive", o));
                    buffer.append(getSNT());
                }
                Optional<String> maximumDateTimeValue = Shaper.dbSchema.getMaximumDateTimeValue(mappedTable, mappedColumn);
                if (maximumDateTimeValue.isPresent()) {
                    o = "\"" + maximumDateTimeValue.get() + "\"" + "^^xsd:dateTime";
                    buffer.append(getPO("sh:maxInclusive", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_DECIMAL:
                // sh:pattern
                Optional<String> decimalRegex = Shaper.dbSchema.getRegexForXSDDecimal(mappedTable, mappedColumn);
                if (decimalRegex.isPresent()) {
                    o = decimalRegex.get().replace("\\", "\\\\");
                    o = Symbols.DOUBLE_QUOTATION_MARK + o + Symbols.DOUBLE_QUOTATION_MARK;
                    buffer.append(getPO("sh:pattern", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_DOUBLE:
                // sh:minInclusive
                boolean isUnsigned = Shaper.dbSchema.isUnsigned(mappedTable, mappedColumn);
                if (isUnsigned) {
                    o = "\"0.0E0\"^^xsd:double";
                    buffer.append(getPO("sh:minInclusive", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_HEX_BINARY:
                // sh:maxLength
                Optional<Integer> maximumOctetLength = Shaper.dbSchema.getMaximumOctetLength(mappedTable, mappedColumn);
                if (maximumOctetLength.isPresent()) {
                    o = Integer.toString(maximumOctetLength.get() * 2);
                    buffer.append(getPO("sh:maxLength", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_INTEGER:
                // sh:minInclusive & sh:maxInclusive
                Optional<String> minimumIntegerValue = Shaper.dbSchema.getMinimumIntegerValue(mappedTable, mappedColumn);
                Optional<String> maximumIntegerValue = Shaper.dbSchema.getMaximumIntegerValue(mappedTable, mappedColumn);
                if (minimumIntegerValue.isPresent() && maximumIntegerValue.isPresent()) {
                    o = minimumIntegerValue.get();
                    buffer.append(getPO("sh:minInclusive", o));
                    buffer.append(getSNT());

                    o = maximumIntegerValue.get();
                    buffer.append(getPO("sh:maxInclusive", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_STRING:
                // sh:maxLength
                Optional<Integer> characterMaximumLength = Shaper.dbSchema.getCharacterMaximumLength(mappedTable, mappedColumn);
                if (characterMaximumLength.isPresent()) {
                    o = characterMaximumLength.get().toString();
                    buffer.append(getPO("sh:maxLength", o));
                    buffer.append(getSNT());
                }
                break;
            case XSD_TIME:
                // sh:pattern
                Optional<String> timeRegex = Shaper.dbSchema.getRegexForXSDTime(mappedTable, mappedColumn);
                if (timeRegex.isPresent()) {
                    o = timeRegex.get().replace("\\", "\\\\");
                    o = Symbols.DOUBLE_QUOTATION_MARK + o + Symbols.DOUBLE_QUOTATION_MARK;
                    buffer.append(getPO("sh:pattern", o));
                    buffer.append(getSNT());
                }
                break;
        }

        // sh:minCount & sh:maxCount
        boolean isNotNull = Shaper.dbSchema.isNotNull(mappedTable, mappedColumn);
        if (isNotNull) {
            o = Integer.toString(1);
            buffer.append(getPO("sh:minCount", o));
            buffer.append(getSNT());
        }
        o = Integer.toString(1);
        buffer.append(getPO("sh:maxCount", o));
        buffer.append(getSNT());

        return buffer.toString();
    }

    private String buildSerializedPropertyShape(ReferenceProperty referenceProperty) {
        StringBuffer buffer = new StringBuffer();

        String o; // to be used as objects of different RDF triples

        // sh:nodeKind
        String mappedTable = referenceProperty.getMappedTable();
        String mappedRefConstraint = referenceProperty.getMappedRefConstraintName();
        String referencedTable = Shaper.dbSchema.getReferencedTableBy(mappedTable, mappedRefConstraint);
        NodeKinds nodeKind;
        if (isInverse)
            nodeKind = (Shaper.dbSchema.getPrimaryKey(mappedTable).size() > 0) ? NodeKinds.IRI : NodeKinds.BlankNode;
        else
            nodeKind = (Shaper.dbSchema.getPrimaryKey(referencedTable).size() > 0) ? NodeKinds.IRI : NodeKinds.BlankNode;
        switch (nodeKind) {
            case BlankNode:
                o = "sh:BlankNode";
                buffer.append(getPO("sh:nodeKind", o));
                buffer.append(getSNT());
                break;
            case IRI:
                o = "sh:IRI";
                buffer.append(getPO("sh:nodeKind", o));
                buffer.append(getSNT());
        }

        // sh:class
        Optional<NodeShape> nodeShape = Optional.empty();
        if (isInverse)
            nodeShape = getShaclDocModel().getMappedNodeShape(mappedTable);
        else
            nodeShape = getShaclDocModel().getMappedNodeShape(referencedTable);
        if (nodeShape.isPresent()) {
            Optional<TableIRI> tableIRI = nodeShape.get().getMappedTableIRI();
            if (tableIRI.isPresent()) {
                URI classIRI = tableIRI.get().getTableIRI();
                o = getShaclDocModel().getRelativeIRIOr(classIRI.toString());
                buffer.append(getPO("sh:class", o));
                buffer.append(getSNT());
            }
        }

        // sh:minCount & sh:maxCount
        if (isInverse == false) {
            List<String> columns = Shaper.dbSchema.getReferencingColumnsByOrdinalPosition(mappedTable, mappedRefConstraint);
            boolean nullable = false;
            for (String column : columns) {
                if (!Shaper.dbSchema.isNotNull(mappedTable, column)) {
                    nullable = true;
                    break;
                }
            }
            if (!nullable) {
                o = Integer.toString(1);
                buffer.append(getPO("sh:minCount", o));
                buffer.append(getSNT());
            }
            o = Integer.toString(1);
            buffer.append(getPO("sh:maxCount", o));
            buffer.append(getSNT());
        }

        return buffer.toString();
    }

    private String buildSerializedPropertyShapeForDirectMapping() {
        StringBuffer buffer = new StringBuffer();

        String o; // to be used as objects of different RDF triples

        // sh:path
        switch (mappingType) {
            case LITERAL_PROPERTY:
                o = getShaclDocModel().getRelativeIRIOr(mappedLiteralProperty.get().getLiteralPropertyIRI());
                buffer.append(getPO("sh:path", o));
                buffer.append(getSNT());
                break;
            case REFERENCE_PROPERTY:
                o = getShaclDocModel().getRelativeIRIOr(mappedReferenceProperty.get().getReferencePropertyIRI());
                if (isInverse)
                    o = Symbols.OPEN_BRACKET + "sh:inversePath" + Symbols.SPACE + o + Symbols.SPACE + Symbols.CLOSE_BRACKET;
                buffer.append(getPO("sh:path", o));
                buffer.append(getSNT());
        }

        // if LiteralProperty
        if (mappingType.equals(MappingTypes.LITERAL_PROPERTY))
            buffer.append(buildSerializedPropertyShape(mappedLiteralProperty.get()));

        // if ReferenceProperty
        if (mappingType.equals(MappingTypes.REFERENCE_PROPERTY))
            buffer.append(buildSerializedPropertyShape(mappedReferenceProperty.get()));

        buffer.setLength(buffer.lastIndexOf(Symbols.SEMICOLON));
        buffer.append(getDNT());

        return buffer.toString();
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private enum MappingTypes { OBJECT_MAP, REF_OBJECT_MAP, LITERAL_PROPERTY, REFERENCE_PROPERTY}

    private PredicateMap mappedPredicateMap;
    private Optional<ObjectMap> mappedObjectMap = Optional.empty();
    private Optional<RefObjectMap> mappedRefObjectMap = Optional.empty();

    private MappingTypes mappingType;

    private boolean hasQualifiedValueShape;

    PropertyShape(URI id, PredicateMap mappedPredicateMap, ObjectMap mappedObjectMap, boolean hasQualifiedValueShape, ShaclDocModel shaclDocModel) {
        super(id, shaclDocModel);
        this.mappedPredicateMap = mappedPredicateMap;
        this.mappedObjectMap = Optional.of(mappedObjectMap);
        this.hasQualifiedValueShape = hasQualifiedValueShape;

        mappingType = MappingTypes.OBJECT_MAP;
    }

    PropertyShape(URI id, PredicateMap mappedPredicateMap, RefObjectMap mappedRefObjectMap, boolean hasQualifiedValueShape, ShaclDocModel shaclDocModel) {
        super(id, shaclDocModel);
        this.mappedPredicateMap = mappedPredicateMap;
        this.mappedRefObjectMap = Optional.of(mappedRefObjectMap);
        this.hasQualifiedValueShape = hasQualifiedValueShape;

        mappingType = MappingTypes.REF_OBJECT_MAP;
    }

    @Override
    public String toString() {
        String serializedPropertyShape = getSerializedShape();
        if (serializedPropertyShape == null) {
            serializedPropertyShape = buildSerializedPropertyShape();
            setSerializedShape(serializedPropertyShape);
        }

        return serializedPropertyShape;
    }

    private String buildSerializedPropertyShape(RefObjectMap mappedRefObjectMap) {
        StringBuffer buffer = new StringBuffer();

        String o; // to be used as objects of different RDF triples

        // sh:node
        URI parentTriplesMap = mappedRefObjectMap.getParentTriplesMap();
        NodeShape mappedNodeShape = getShaclDocModel().getMappedNodeShape(parentTriplesMap);
        o = getShaclDocModel().getRelativeIRIOr(mappedNodeShape.getID().toString());
        buffer.append(getPO("sh:node", o));
        buffer.append(getSNT());

        return buffer.toString();
    }

    private Optional<NodeKinds> getNodeKind() {
        if (mappedObjectMap.isPresent()) {
            Optional<TermMap.TermTypes> termType = mappedObjectMap.get().getTermType();
            if (termType.isPresent()) {
                switch (termType.get()) {
                    case BLANKNODE: return Optional.of(NodeKinds.BlankNode);
                    case IRI: return Optional.of(NodeKinds.IRI);
                    case LITERAL: return Optional.of(NodeKinds.Literal);
                }
            }
        }

        return Optional.empty();
    }

    private Optional<URI> getDatatype() {
        if (mappedObjectMap.isPresent()) {
            Optional<URI> datatype = mappedObjectMap.get().getDatatype();

            if (datatype.isPresent()) return Optional.of(datatype.get());

            boolean isBlankNodeOrIRI = false;
            Optional<NodeKinds> nodeKind = getNodeKind();
            if (nodeKind.isPresent()) {
                switch (nodeKind.get()) {
                    case BlankNode:
                    case IRI:
                        isBlankNodeOrIRI = true;
                }
            }

            // only if it's a Literal
            if (!isBlankNodeOrIRI) {
                // Natural Mapping of SQL Values
                Optional<SQLSelectField> sqlSelectField = mappedObjectMap.get().getColumn();
                if (sqlSelectField.isPresent())
                    return Optional.of(SqlXsdMap.getMappedXSD(sqlSelectField.get().getSqlType()).getURI());
            }
        }

        return Optional.empty();
    }

    private String buildSerializedPropertyShape(ObjectMap mappedObjectMap, boolean hasQualifiedValueShape) {
        StringBuffer buffer = new StringBuffer();

        String o; // to be used as objects of different RDF triples

        // sh:nodeKind
        Optional<NodeKinds> nodeKind = getNodeKind();
        if (nodeKind.isPresent()) {
            switch (nodeKind.get()) {
                case BlankNode: o = "sh:BlankNode"; break;
                case IRI: o = "sh:IRI"; break;
                case Literal: o = "sh:Literal"; break;
                default: o = null;
            }

            if (o != null) {
                buffer.append(getPO("sh:nodeKind", o));
                buffer.append(getSNT());
            }
        }

        // sh:in
        Optional<String> constant = mappedObjectMap.getConstant();
        if (constant.isPresent()) {
            o = constant.get();

            if (nodeKind.isPresent()) {
                switch (nodeKind.get()) {
                    case IRI: o = getShaclDocModel().getRelativeIRIOr(o); break;
                    case Literal: o = Symbols.DOUBLE_QUOTATION_MARK + o + Symbols.DOUBLE_QUOTATION_MARK; break;
                }
            }

            o = Symbols.OPEN_PARENTHESIS + Symbols.SPACE + o + Symbols.SPACE + Symbols.CLOSE_PARENTHESIS;

            if (hasQualifiedValueShape)
                buffer.append(getPO("sh:qualifiedValueShape", getUBN("sh:in", o)));
            else
                buffer.append(getPO("sh:in", o));

            buffer.append(getSNT());
        }

        // sh:languageIn
        Optional<String> language = mappedObjectMap.getLanguage();
        if (language.isPresent()) {
            o = Symbols.OPEN_PARENTHESIS + Symbols.SPACE + Symbols.DOUBLE_QUOTATION_MARK + language.get() + Symbols.DOUBLE_QUOTATION_MARK + Symbols.SPACE + Symbols.CLOSE_PARENTHESIS;
            buffer.append(getPO("sh:languageIn", o));
            buffer.append(getSNT());
        }

        // sh:datatype
        o = null;

        Optional<URI> datatype = getDatatype();
        if (language.isEmpty() && datatype.isPresent()) {
            o = getShaclDocModel().getRelativeIRIOr(datatype.get());

            if (o != null) {
                buffer.append(getPO("sh:datatype", o));
                buffer.append(getSNT());
            }
        }

        // sh:maxLength
        Optional<SQLSelectField> sqlSelectField = mappedObjectMap.getColumn();
        if (sqlSelectField.isPresent()) {
            if (nodeKind.isPresent() && nodeKind.get().equals(NodeKinds.Literal)) {
                if (language.isPresent() ||
                        (datatype.isPresent() && datatype.get().equals(XSDs.XSD_STRING.getURI()))) {
                    o = Integer.toString(sqlSelectField.get().getDisplaySize());

                    buffer.append(getPO("sh:maxLength", o));
                    buffer.append(getSNT());
                }
            }
        }

        // sh:pattern
        Optional<String> regex = getRegex(mappedObjectMap);
        if (regex.isPresent()) {
            o = regex.get();

            if (hasQualifiedValueShape)
                buffer.append(getPO("sh:qualifiedValueShape", getUBN("sh:pattern", o)));
            else
                buffer.append(getPO("sh:pattern", o));

            buffer.append(getSNT());
        }

        // cardinality
        o = "1";
        // cardinality: rr:column
        if (sqlSelectField.isPresent()) {
            switch (sqlSelectField.get().getNullable()) {
                case ResultSetMetaData.columnNoNulls:
                    // "at least one"
                    if (hasQualifiedValueShape)
                        buffer.append(getPO("sh:qualifiedMinCount", o));
                    else
                        buffer.append(getPO("sh:minCount", o));
                    buffer.append(getSNT());
            }
        }
        // cardinality: rr:template
        Optional<Template> template = mappedObjectMap.getTemplate();
        if (template.isPresent()) {
            List<SQLSelectField> columnNames = template.get().getColumnNames();
            boolean isEveryColumnNoNulls = true;
            for (SQLSelectField columnName: columnNames) {
                // Is a nullable column?
                if (columnName.getNullable() != ResultSetMetaData.columnNoNulls) {
                    isEveryColumnNoNulls = false;
                    break;
                }
            }
            // "at least one"
            if (isEveryColumnNoNulls) {
                if (hasQualifiedValueShape)
                    buffer.append(getPO("sh:qualifiedMinCount", o));
                else
                    buffer.append(getPO("sh:minCount", o));
                buffer.append(getSNT());
            }
        }
        // cardinality: rr:constant or rr:object
        if (constant.isPresent()) {
            // "exactly one"
            if (hasQualifiedValueShape)
                buffer.append(getPO("sh:qualifiedMinCount", o));
            else
                buffer.append(getPO("sh:minCount", o));
            buffer.append(getSNT());
            if (hasQualifiedValueShape)
                buffer.append(getPO("sh:qualifiedMaxCount", o));
            else
                buffer.append(getPO("sh:maxCount", o));
            buffer.append(getSNT());
        }

        // sh:qualifiedValueShapesDisjoint
        if (hasQualifiedValueShape) {
            buffer.append(getPO("sh:qualifiedValueShapesDisjoint", "true"));
            buffer.append(getSNT());
        }

        return buffer.toString();
    }

    private String buildSerializedPropertyShape() {
        StringBuffer buffer = new StringBuffer();

        String o; // to be used as objects of different RDF triples

        // rdf:type sh:PropertyShape
        String id = getShaclDocModel().getRelativeIRIOr(getID().toString());
        buffer.append(id);
        buffer.append(getNT());

        buffer.append(getPO(Symbols.A, "sh:PropertyShape"));
        buffer.append(getSNT());

        switch (mappingType) {
            case OBJECT_MAP:
            case REF_OBJECT_MAP:
                buffer.append(buildSerializedPropertyShapeForR2RML());
                break;
            case LITERAL_PROPERTY:
            case REFERENCE_PROPERTY:
                buffer.append(buildSerializedPropertyShapeForDirectMapping());
        }

        return buffer.toString();
    }

    private String buildSerializedPropertyShapeForR2RML() {
        StringBuffer buffer = new StringBuffer();

        String o; // to be used as objects of different RDF triples

        // sh:path
        o = getShaclDocModel().getRelativeIRIOr(mappedPredicateMap.getConstant().get());
        buffer.append(getPO("sh:path", o));
        buffer.append(getSNT());

        // if RefObjectMap
        if (mappingType.equals(MappingTypes.REF_OBJECT_MAP))
            buffer.append(buildSerializedPropertyShape(mappedRefObjectMap.get()));

        // if ObjectMap
        if (mappingType.equals(MappingTypes.OBJECT_MAP))
            buffer.append(buildSerializedPropertyShape(mappedObjectMap.get(), hasQualifiedValueShape));

        buffer.setLength(buffer.lastIndexOf(Symbols.SEMICOLON));
        buffer.append(getDNT());

        return buffer.toString();
    }

    private Optional<String> getRegex(ObjectMap objectMap) {
        Optional<Template> template = objectMap.getTemplate();

        if (!isPossibleToHavePattern(template)) return Optional.empty();

        String regex = template.get().getFormat();

        // column names
        List<SQLSelectField> columnNames = template.get().getColumnNames();
        for (SQLSelectField columnName: columnNames) {
            String replacement = "(.*)";

            Optional<NodeKinds> nodeKind = getNodeKind();
            if (nodeKind.isPresent() && nodeKind.get().equals(NodeKinds.Literal)) {
                int displaySize = columnName.getDisplaySize();
                replacement = "(.{0," + displaySize + "})";
            }

            regex = regex.replace("{" + columnName.getColumnNameOrAlias() + "}", replacement);
        }

        // because backslashes need to be escaped by a second backslash in the Turtle syntax,
        // a double backslash is needed to escape each curly brace,
        // and to get one literal backslash in the output one needs to write four backslashes in the template.
        regex = regex.replace("\\\\", "\\");

        regex = regex.replace("\\{", "{");
        regex = regex.replace("\\}", "}");

        return Optional.of(Symbols.DOUBLE_QUOTATION_MARK + Symbols.CARET + regex + Symbols.DOLLAR + Symbols.DOUBLE_QUOTATION_MARK);
    }
}
