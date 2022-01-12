package shaper.mapping.model.shex;

import shaper.Shaper;
import shaper.mapping.SqlXsdMap;
import shaper.mapping.Symbols;
import shaper.mapping.XSDs;
import shaper.mapping.model.ID;

import java.util.Optional;
import java.util.Set;

public class DMNodeConstraint extends NodeConstraint {
    private String mappedTable;
    private String mappedColumn;

    DMNodeConstraint(ID id, String mappedTable, String mappedColumn) {
        super(id);
        this.mappedTable = mappedTable;
        this.mappedColumn = mappedColumn;
    }

    String getMappedTable() {
        return mappedTable;
    }

    String getMappedColumn() {
        return mappedColumn;
    }

    private void buildNodeConstraint(String table, String column) {
        setValues(buildValueSet(table, column));
        if (getValues().isPresent())
            return;

        XSDs xsdType = SqlXsdMap.getMappedXSD(Shaper.dbSchema.getJDBCDataType(table, column));
        setDatatype(Optional.of(xsdType.getRelativeIRI()));

        setXsFacet(buildFacet(table, column, xsdType));
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

    private String getWrappedRegExWithSlash(String regEx) { return Symbols.SLASH + regEx + Symbols.SLASH; }

    @Override
    public String toString() {
        String serializedNodeConstraint = getSerializedNodeConstraint();

        if (serializedNodeConstraint == null) {
            buildNodeConstraint(mappedTable, mappedColumn);
            buildSerializedNodeConstraint();
            serializedNodeConstraint = getSerializedNodeConstraint();

        }

        return serializedNodeConstraint;
    }
}
