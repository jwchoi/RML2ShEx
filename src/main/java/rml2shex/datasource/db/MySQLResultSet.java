package rml2shex.datasource.db;

import rml2shex.util.SqlXsdMap;
import rml2shex.util.Symbols;
import rml2shex.util.XSDs;

import javax.xml.bind.DatatypeConverter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Vector;

public class MySQLResultSet extends SQLResultSet {
    private Optional<ZoneOffset> dbZoneOffset;
    private List<Integer> timeZoneAwareTemporalDataTypeColumnIndexes;

    MySQLResultSet(ResultSet rs, ResultSetMetaData rsmd, Optional<ZoneOffset> dbZoneOffset, List<Integer> timeZoneAwareTemporalDataTypeColumnIndexes) {
        super(rs, rsmd);
        this.dbZoneOffset = dbZoneOffset;
        this.timeZoneAwareTemporalDataTypeColumnIndexes = timeZoneAwareTemporalDataTypeColumnIndexes;
    }

    @Override
    public List<String> getResultSetRowAt(int row) {
        int cnt = getResultSetColumnCount();

        List<String> v = new Vector<>(cnt);

        try {
            rs.absolute(row);

            for (int i = 1 ; i <= cnt; i++) {
                if (SqlXsdMap.getMappedXSD(rsmd.getColumnType(i)).equals(XSDs.XSD_HEX_BINARY))
                    v.add(DatatypeConverter.printHexBinary(rs.getBytes(i)));
                else if (SqlXsdMap.getMappedXSD(rsmd.getColumnType(i)).equals(XSDs.XSD_DATE_TIME)) {
                    String value = rs.getString(i);
                    value = value.trim().replace(Symbols.SPACE, "T");
                    value = adjustScale(value, rsmd.getScale(i));
                    if (timeZoneAwareTemporalDataTypeColumnIndexes.contains(i) && dbZoneOffset.isPresent()) {
                        value = value + dbZoneOffset.get();
                        value = OffsetDateTime.parse(value).toInstant().toString();
                    }
                    v.add(value);
                }
                else
                    v.add(rs.getString(i));
            }
        } catch(SQLException e) { e.printStackTrace(); }

        return v;
    }

    // when scale is 0, remove ".0"
    private String adjustScale(String value, int scale) {
        if (scale == 0 && value.endsWith(".0"))
            value = value.substring(0, value.lastIndexOf(".0"));

        return value;
    }
}
