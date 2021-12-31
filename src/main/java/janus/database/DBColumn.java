package janus.database;

public class DBColumn implements Comparable<DBColumn> {
	
	private String tableName;
	private String columnName;
	
	public DBColumn(String tableName, String columnName) {
		this.tableName = tableName;
		this.columnName = columnName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String getColumnName() {
		return columnName;
	}

	@Override
	public int compareTo(DBColumn o) {
		int betweenTables = tableName.compareTo(o.getTableName());
		
		if (betweenTables != 0)
			return betweenTables;
		
		return columnName.compareTo(o.getColumnName());
	}
	
	@Override
	public String toString() {
		return tableName + "." + columnName;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DBColumn) {
			if (((DBColumn)obj).getTableName().equals(tableName) 
					&& ((DBColumn)obj).getColumnName().equals(columnName))
				return true;
			else
				return false;
		}
		
		return super.equals(obj);
	}
}
