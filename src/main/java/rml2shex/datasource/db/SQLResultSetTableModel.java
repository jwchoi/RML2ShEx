package rml2shex.datasource.db;

import javax.swing.table.AbstractTableModel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class SQLResultSetTableModel extends AbstractTableModel {
	
	private int columnCount;
	private int rowCount;
	private CachedRecord cache;
	
	private SQLResultSet resultSet;

	public SQLResultSetTableModel(SQLResultSet resultSet) {
		this.resultSet = resultSet;
		
		rowCount = resultSet.getResultSetRowCount();
		columnCount = resultSet.getResultSetColumnCount();
		
		cache = new CachedRecord();
	}
	
	@Override
	public String getColumnName(int column) {
		return resultSet.getColumnName(column + 1);
	}

	public int getRowCount() {
		return rowCount;
	}

	public int getColumnCount() {
		return columnCount;
	}

	public Object getValueAt(int rowIndex, int columnIndex) {
		
		Record record = cache.get(rowIndex);

		if (record == null) {
			record = getRecord(rowIndex);
			cache.put(rowIndex, record);
		}

		return record.get(columnIndex);
	}

	private Record getRecord(int rowIndex) {
		return new Record(resultSet.getResultSetRowAt(rowIndex + 1));
	}

	private class Record {
		private List<String> record;
		
		Record(List<String> record) {
			this.record = record;
		}
		
		String get(int columnIndex) {
			return record.get(columnIndex);
		}
	}

	private class CachedRecord extends LinkedHashMap<Integer,Record> {
		private static final int MAX_ENTRIES = 100;

		@Override
		protected boolean removeEldestEntry(Entry<Integer, Record> eldest) {
			return size() > MAX_ENTRIES;
		}
	}
}
