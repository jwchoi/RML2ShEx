package shaper.mapping.model.dm;

import shaper.Shaper;
import shaper.mapping.SqlXsdMap;
import shaper.mapping.Symbols;
import shaper.mapping.XSDs;

class Literal {

	// referenced: https://www.w3.org/TR/2012/REC-r2rml-20120927/#natural-mapping
    static String getMappedLiteral(String table, String column, String value) {
        String literal;

	    int sqlDataType = Shaper.dbSchema.getJDBCDataType(table, column);
        XSDs xmlSchemaDataType = SqlXsdMap.getMappedXSD(sqlDataType);

        switch (xmlSchemaDataType) {
            case XSD_STRING:
                literal = getStringLiteral(value);
                break;
            case XSD_DATE_TIME:
                value = value.replace(Symbols.SPACE, "T");
                literal = buildTypedLiteral(value, xmlSchemaDataType);
                break;
            case XSD_BOOLEAN:
                value = value.toLowerCase();
                if (value.equals("0")) value = "false";
                if (value.equals("1")) value = "true";
                literal = buildTypedLiteral(value, xmlSchemaDataType);
                break;
            default:
                literal = buildTypedLiteral(value, xmlSchemaDataType);
        }

        return literal;
    }

    private static String buildTypedLiteral(String lexicalValue, XSDs anXsd) {
        return Symbols.DOUBLE_QUOTATION_MARK + lexicalValue + Symbols.DOUBLE_QUOTATION_MARK  + Symbols.CARET + Symbols.CARET + anXsd.getRelativeIRI();
    }

    // https://www.w3.org/TR/turtle/#literals
    private static String getStringLiteral(String value) {
	    String qMark = Symbols.DOUBLE_QUOTATION_MARK;
	    if (value.contains(qMark))
	        qMark = Symbols.SINGLE_QUOTATION_MARK;

	    if (value.contains(Symbols.NEWLINE))
	        return qMark + qMark + qMark + value + qMark + qMark + qMark;
	    else
	        return qMark + value + qMark;
    }
}
