package janus.database;

public class DBField implements Comparable<DBField> {
	
	private String tableName;
	private String columnName;
	private String value;
	
	public DBField(String tableName, String columnName, String value) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.value = value;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public String getValue() {
		return value;
	}

	@Override
	public int compareTo(DBField o) {
		return columnName.compareTo(o.getColumnName());
	}
	
	@Override
	public String toString() {
		return tableName + "." + columnName + " = " + "'" + value + "'";
	}
	
	public String getNotEqualExpression() {
		return tableName + "." + columnName + " <> " + "'" + value + "'";
	}
}
