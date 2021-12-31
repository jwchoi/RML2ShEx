package janus.database;

public class SQLSelectField {
    private String columnNameOrAlias;
    private String selectQuery;
    private int nullable; // 0: ResultSetMetaData.columnNoNulls, 1: ResultSetMetaData.columnNullable, 2: ResultSetMetaData.columnNullableUnknown
    private int sqlType;  // SQL type
    private int displaySize; // Indicates the designated column's normal maximum width in characters.

    public SQLSelectField(String columnNameOrAlias, String selectQuery) {
        this.columnNameOrAlias = columnNameOrAlias;
        this.selectQuery = selectQuery;
    }

    public void setNullable(int nullable) { this.nullable = nullable; }
    public void setSqlType(int sqlType) { this.sqlType = sqlType; }
    public void setDisplaySize(int displaySize) { this.displaySize = displaySize; }

    public int getSqlType() { return sqlType; }
    public int getNullable() { return nullable; }
    public String getColumnNameOrAlias() { return columnNameOrAlias; }
    public int getDisplaySize() { return displaySize; }
}
