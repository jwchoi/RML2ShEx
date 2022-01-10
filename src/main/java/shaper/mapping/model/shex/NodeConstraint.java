package shaper.mapping.model.shex;

import janus.database.SQLSelectField;
import shaper.Shaper;
import shaper.mapping.SqlXsdMap;
import shaper.mapping.Symbols;
import shaper.mapping.XSDs;
import shaper.mapping.model.r2rml.ObjectMap;
import shaper.mapping.model.r2rml.Template;
import shaper.mapping.model.r2rml.TermMap;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class NodeConstraint extends ShapeExpr implements Comparable<NodeConstraint> {

    private enum XSFacets {
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

    private String nodeConstraint;
    private String id;

    private String mappedTable;
    private String mappedColumn;

    NodeConstraint(String mappedTable, String mappedColumn) {
        this();
        this.mappedTable = mappedTable;
        this.mappedColumn = mappedColumn;
        this.id = buildNodeConstraintID(mappedTable, mappedColumn);
    }

    String getNodeConstraintID() { return id; }

    String getMappedTable() {
        return mappedTable;
    }

    String getMappedColumn() {
        return mappedColumn;
    }

    private String buildNodeConstraintID(String mappedTable, String mappedColumn) {
        return mappedTable + Character.toUpperCase(mappedColumn.charAt(0)) + mappedColumn.substring(1);
    }

    @Override
    public String toString() {
        if (nodeConstraint == null) {

            if (mappedTable != null && mappedColumn != null)
                buildNodeConstraint(mappedTable, mappedColumn);
            else
                buildNodeConstraint(mappedObjectMap);

            StringBuffer nodeConstraint = new StringBuffer();

            if (valueSet.isPresent())
                nodeConstraint.append(valueSet.get());
            else if (datatype.isPresent())
                nodeConstraint.append(datatype.get());
            else if (nodeKind.isPresent())
                nodeConstraint.append(nodeKind.get());

            if (xsFacet.isPresent())
                nodeConstraint.append(Symbols.SPACE + xsFacet.get());

            this.nodeConstraint = nodeConstraint.toString();
        }

        return this.nodeConstraint;
    }

    private void buildNodeConstraint(String table, String column) {
        valueSet = buildValueSet(table, column);
        if (valueSet.isPresent())
            return;

        XSDs xsdType = SqlXsdMap.getMappedXSD(Shaper.dbSchema.getJDBCDataType(table, column));
        datatype = Optional.of(xsdType.getRelativeIRI());

        xsFacet = buildFacet(table, column, xsdType);
    }

    private Optional<String> buildValueSet(String table, String column) {
        Optional<Set<String>> valueSet = Shaper.dbSchema.getValueSet(table, column);

        if (valueSet.isPresent()) {
            StringBuffer buffer = new StringBuffer(Symbols.OPEN_BRACKET + Symbols.SPACE);

            Set<String> set = valueSet.get();
            for (String value: set) {
                if (value.startsWith(Symbols.SINGLE_QUOTATION_MARK) && value.endsWith(Symbols.SINGLE_QUOTATION_MARK)) {
                    value = value.substring(1, value.length()-1);
                    value = Symbols.DOUBLE_QUOTATION_MARK + value + Symbols.DOUBLE_QUOTATION_MARK;
                }
                buffer.append(value + Symbols.SPACE);
            }
            buffer.append(Symbols.CLOSE_BRACKET);

            return Optional.of(buffer.toString());
        } else
            return Optional.empty();
    }

    private String getWrappedRegExWithSlash(String regEx) { return Symbols.SLASH + regEx + Symbols.SLASH; }

    private Optional<String> buildFacet(String table, String column, XSDs xsd) {
        String facet = null;
        switch (xsd) {
            case XSD_BOOLEAN:
                facet = getWrappedRegExWithSlash("true|false");
                break;
            case XSD_DATE:
                facet = getWrappedRegExWithSlash(Shaper.dbSchema.getRegexForXSDDate());
                break;
            case XSD_DATE_TIME:
                facet = getWrappedRegExWithSlash(Shaper.dbSchema.getRegexForXSDDateTime(table, column).get());
                break;
            case XSD_DECIMAL:
                Integer numericPrecision = Shaper.dbSchema.getNumericPrecision(table, column).get();
                Integer numericScale = Shaper.dbSchema.getNumericScale(table, column).get();
                facet = XSFacets.TOTAL_DIGITS + Symbols.SPACE + numericPrecision + Symbols.SPACE + XSFacets.FRACTION_DIGITS + Symbols.SPACE + numericScale;
                break;
            case XSD_DOUBLE:
                boolean isUnsigned = Shaper.dbSchema.isUnsigned(table, column);
                facet = isUnsigned ? XSFacets.MIN_INCLUSIVE + Symbols.SPACE + Symbols.ZERO : null;
                break;
            case XSD_HEX_BINARY:
                Integer maximumOctetLength = Shaper.dbSchema.getMaximumOctetLength(table, column).get();
                facet = XSFacets.MAX_LENGTH + Symbols.SPACE + (maximumOctetLength*2);
                break;
            case XSD_INTEGER:
                String minimumIntegerValue = Shaper.dbSchema.getMinimumIntegerValue(table, column).get();
                String maximumIntegerValue = Shaper.dbSchema.getMaximumIntegerValue(table, column).get();
                facet = XSFacets.MIN_INCLUSIVE + Symbols.SPACE + minimumIntegerValue + Symbols.SPACE + XSFacets.MAX_INCLUSIVE + Symbols.SPACE + maximumIntegerValue;
                break;
            case XSD_STRING:
                Integer characterMaximumLength = Shaper.dbSchema.getCharacterMaximumLength(table, column).get();
                facet = XSFacets.MAX_LENGTH + Symbols.SPACE + characterMaximumLength;
                break;
            case XSD_TIME:
                facet = getWrappedRegExWithSlash(Shaper.dbSchema.getRegexForXSDTime(table, column).get());
                break;
        }

        return Optional.ofNullable(facet);
    }

    @Override
    public int compareTo(NodeConstraint o) {
        return nodeConstraint.compareTo(o.toString());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private Optional<NodeKinds> nodeKind;
    private Optional<String> valueSet;
    private Optional<String> datatype;
    private Optional<String> xsFacet;

    private ObjectMap mappedObjectMap;

    private NodeConstraint() {
        nodeKind = Optional.empty();
        valueSet = Optional.empty();
        datatype = Optional.empty();
        xsFacet = Optional.empty();
    }

    NodeConstraint(String id, ObjectMap mappedObjectMap) {
        this();
        this.id = id;
        this.mappedObjectMap = mappedObjectMap;
    }

    ObjectMap getMappedObjectMap() { return mappedObjectMap; }

    public static boolean isPossibleToHaveXSFacet(ObjectMap objectMap) {
        Optional<Template> template = objectMap.getTemplate();
        if (template.isPresent()) {
            if (template.get().getLengthExceptColumnName() > 0)
                return true;
        }

        return false;
    }

    private void buildNodeConstraint(ObjectMap objectMap) {
        // nodeKind
        Optional<TermMap.TermTypes> termType = objectMap.getTermType();
        if (termType.isPresent()) {
            if (termType.get().equals(TermMap.TermTypes.BLANKNODE))
                nodeKind = Optional.of(NodeKinds.BNODE);
            else if (termType.get().equals(TermMap.TermTypes.IRI))
                nodeKind = Optional.of(NodeKinds.IRI);
            else if (termType.get().equals(TermMap.TermTypes.LITERAL))
                nodeKind = Optional.of(NodeKinds.LITERAL);
        }

        // language
        Optional<String> language = objectMap.getLanguage();
        if (language.isPresent()) {
            String languageTag = Symbols.OPEN_BRACKET + Symbols.AT + language.get() + Symbols.CLOSE_BRACKET;
            valueSet = Optional.of(languageTag);
        }

        // datatype
        Optional<URI> datatype = objectMap.getDatatype();
        if (datatype.isPresent()) { // from rr:column
            String dt = datatype.get().toString();
            Optional<String> relativeDatatype = Shaper.shexMapper.r2rmlModel.getRelativeIRI(datatype.get());
            if (relativeDatatype.isPresent())
                dt = relativeDatatype.get();

            this.datatype = Optional.of(dt);
        } else { // Natural Mapping of SQL Values
            Optional<SQLSelectField> sqlSelectField = objectMap.getColumn();
            if (sqlSelectField.isPresent())
                this.datatype = Optional.of(SqlXsdMap.getMappedXSD(sqlSelectField.get().getSqlType()).getRelativeIRI());
        }

        // xsFacet, only REGEXP
        if (isPossibleToHaveXSFacet(objectMap))
            xsFacet = Optional.of(buildRegex(objectMap.getTemplate().get()));
    }

    private String buildRegex(Template template) {
        String regex = template.getFormat();

        // replace meta-characters in XPath
        regex = regex.replace(Symbols.SLASH, Symbols.BACKSLASH + Symbols.SLASH);
        regex = regex.replace(Symbols.DOT, Symbols.BACKSLASH + Symbols.DOT);

        // column names
        List<SQLSelectField> columnNames = template.getColumnNames();
        for (SQLSelectField columnName: columnNames)
            regex = regex.replace("{" + columnName.getColumnNameOrAlias() + "}", "(.*)");

        return Symbols.SLASH + Symbols.CARET + regex + Symbols.DOLLAR + Symbols.SLASH;
    }
}
