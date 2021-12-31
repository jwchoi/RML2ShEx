package shaper.mapping;

import java.util.Hashtable;
import java.util.Map;

// nm: Based on: https://www.w3.org/TR/2012/REC-r2rml-20120927/#natural-mapping
// ws: Based on: https://www.ibm.com/support/knowledgecenter/SSRTLW_8.5.1/com.ibm.datatools.dsws.tooling.ui.rad.doc/topics/rdswssupdattyp.html
// mapping: Based on: https://www.w3.org/2001/sw/rdb2rdf/wiki/Mapping_SQL_datatypes_to_XML_Schema_datatypes
public final class SqlXsdMap {

	private static Map<Integer, XSDs> map = new Hashtable<>(36);
	
	static {
		map.put(Integer.valueOf(java.sql.Types.ARRAY), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.BIGINT), XSDs.XSD_INTEGER); // nm
		map.put(Integer.valueOf(java.sql.Types.BINARY), XSDs.XSD_HEX_BINARY); // nm
		map.put(Integer.valueOf(java.sql.Types.BIT), XSDs.XSD_BOOLEAN); // nm
		map.put(Integer.valueOf(java.sql.Types.BLOB), XSDs.XSD_HEX_BINARY); // nm
		map.put(Integer.valueOf(java.sql.Types.BOOLEAN), XSDs.XSD_BOOLEAN); // nm
		map.put(Integer.valueOf(java.sql.Types.CHAR), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.CLOB), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.DATALINK), XSDs.XSD_ANY_URI); // ws
		map.put(Integer.valueOf(java.sql.Types.DATE), XSDs.XSD_DATE); // nm
		map.put(Integer.valueOf(java.sql.Types.DECIMAL), XSDs.XSD_DECIMAL); // nm
		map.put(Integer.valueOf(java.sql.Types.DISTINCT), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.DOUBLE), XSDs.XSD_DOUBLE); // nm
		map.put(Integer.valueOf(java.sql.Types.FLOAT), XSDs.XSD_DOUBLE); // nm
		map.put(Integer.valueOf(java.sql.Types.INTEGER), XSDs.XSD_INTEGER); // nm
		map.put(Integer.valueOf(java.sql.Types.JAVA_OBJECT), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.LONGNVARCHAR), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.LONGVARBINARY), XSDs.XSD_HEX_BINARY); // nm
		map.put(Integer.valueOf(java.sql.Types.LONGVARCHAR), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.NCHAR), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.NCLOB), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.NULL), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.NUMERIC), XSDs.XSD_DECIMAL); // nm
		map.put(Integer.valueOf(java.sql.Types.NVARCHAR), XSDs.XSD_STRING); // nm
		map.put(Integer.valueOf(java.sql.Types.OTHER), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.REAL), XSDs.XSD_DOUBLE); // nm
		map.put(Integer.valueOf(java.sql.Types.REF), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.REF_CURSOR), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.ROWID), XSDs.XSD_BASE_64_BINARY); // ws
		map.put(Integer.valueOf(java.sql.Types.SMALLINT), XSDs.XSD_INTEGER); // nm
		map.put(Integer.valueOf(java.sql.Types.SQLXML), XSDs.XSD_ANY_TYPE); // ws
		map.put(Integer.valueOf(java.sql.Types.STRUCT), XSDs.XSD_STRING); // ws
		map.put(Integer.valueOf(java.sql.Types.TIME), XSDs.XSD_TIME); // nm
		map.put(Integer.valueOf(java.sql.Types.TIME_WITH_TIMEZONE), XSDs.XSD_TIME); // nm
		map.put(Integer.valueOf(java.sql.Types.TIMESTAMP), XSDs.XSD_DATE_TIME); // nm
		map.put(Integer.valueOf(java.sql.Types.TIMESTAMP_WITH_TIMEZONE), XSDs.XSD_DATE_TIME); // nm
		map.put(Integer.valueOf(java.sql.Types.TINYINT), XSDs.XSD_INTEGER); // nm
		map.put(Integer.valueOf(java.sql.Types.VARBINARY), XSDs.XSD_HEX_BINARY); // nm
		map.put(Integer.valueOf(java.sql.Types.VARCHAR), XSDs.XSD_STRING); // nm
	}
	
	public static XSDs getMappedXSD(int key) {
		return map.get(Integer.valueOf(key));
	}
}
