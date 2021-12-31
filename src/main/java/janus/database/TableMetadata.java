package janus.database;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArraySet;

class TableMetadata implements Comparable<TableMetadata> {
	private String tableName;

	private String superTableName;
	
	private List<ColumnMetadata> columnMetaDataSet;

    private Set<RefConstraintMetadata> refConstraintSet;

    private Set<UniqueConstraintMetadata> uniqueConstraintSet;

    private List<String> primaryKey;
	
	TableMetadata(String tableName) {
		this.tableName = tableName;
		
		columnMetaDataSet = new Vector<>();
		refConstraintSet = new CopyOnWriteArraySet<>();
		uniqueConstraintSet = new CopyOnWriteArraySet<>();
	}

	String getReferencedTableBy(String refConstraintName) {
		for (RefConstraintMetadata refConstraint: refConstraintSet)
			if (refConstraint.getConstraintName().equals(refConstraintName))
				return refConstraint.getReferencedTableName();

		return null;
	}

	boolean isAutoIncrement(String columnName) {
        for (ColumnMetadata columnMetaData: columnMetaDataSet)
            if (columnMetaData.getColumnName().equals(columnName))
                return columnMetaData.isAutoIncrement();

        return false;
    }

	void setPrimaryKey(List<String> primaryKey) { this.primaryKey = primaryKey; }

	List<String> getPrimaryKey() { return primaryKey; }

	void addReferentialConstraint(RefConstraintMetadata refConstraint) {
	    refConstraintSet.add(refConstraint);
    }

    Set<String> getRefConstraints() {
		Set<String> set = new CopyOnWriteArraySet<>();

		for (RefConstraintMetadata refConstraint: refConstraintSet)
		    set.add(refConstraint.getConstraintName());

	    return set;
	}

	List<String> getReferencingColumnsByOrdinalPosition(String refConstraintName) {
        for (RefConstraintMetadata refConstraint: refConstraintSet)
            if (refConstraint.getConstraintName().equals(refConstraintName))
                return refConstraint.getReferencingColumnsByOrdinalPosition();

        return null;
    }

	String getReferencedColumnsBy(String refConstraintName, String referencingColumnName) {
		for (RefConstraintMetadata refConstraint: refConstraintSet)
			if (refConstraint.getConstraintName().equals(refConstraintName))
				return refConstraint.getReferencedColumnNameBy(referencingColumnName);

		return null;
	}

    void addUniqueConstraint(UniqueConstraintMetadata uniqueConstraint) {
        uniqueConstraintSet.add(uniqueConstraint);
    }
	
	String getSuperColumn(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.getSuperColumn();
		
		return null;
	}
	
	boolean isForeignKey(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.isForeignKey();
		
		return false;
	}
	
	boolean isSingleColumnUniqueKey(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.isSingleColumnUniqueKey();
		
		return false;
	}
	
	boolean isPrimaryKey(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.isPrimaryKey();
		
		return false;
	}
	
	boolean isNotNull(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.isNotNull();
		
		return false;
	}
	
	boolean isKey(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.isKey();
		
		return false;
	}
	
	int getJDBCDataType(String columnName) {
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				return columnMetaData.getJDBCDataType();
		
		return 2012;
	}

	String getColumnType(String columnName) {
	    String typeName = null;

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				typeName = columnMetaData.getColumnType();

		return typeName;
	}

	Optional<Set<String>> getValueSet(String catalogName, String columnName) {
		Optional<Set<String>> valueSet = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				valueSet = columnMetaData.getValueSet(catalogName, tableName);

		return valueSet;
	}

	boolean isUnsigned(String catalogName, String columnName) {
		boolean isUnsigned = false;

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				isUnsigned = columnMetaData.isUnsigned(catalogName, tableName);

		return isUnsigned;
	}

	Optional<Integer> getCharacterMaximumLength(String catalogName, String columnName) {
		Optional<Integer> characterMaximumLength = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				characterMaximumLength = columnMetaData.getCharacterMaximumLength(catalogName, tableName);

		return characterMaximumLength;
	}

	Optional<Integer> getMaximumOctetLength(String catalogName, String columnName) {
		Optional<Integer> maximumOctetLength = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				maximumOctetLength = columnMetaData.getMaximumOctetLength(catalogName, tableName);

		return maximumOctetLength;
	}

	Optional<Integer> getNumericScale(String catalogName, String columnName) {
		Optional<Integer> numericScale = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				numericScale = columnMetaData.getNumericScale(catalogName, tableName);

		return numericScale;
	}

	Optional<Integer> getNumericPrecision(String catalogName, String columnName) {
		Optional<Integer> numericPrecision = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				numericPrecision = columnMetaData.getNumericPrecision(catalogName, tableName);

		return numericPrecision;
	}

	Optional<String> getMaximumIntegerValue(String catalogName, String columnName) {
		Optional<String> maximumIntegerValue = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				maximumIntegerValue = columnMetaData.getMaximumIntegerValue(catalogName, tableName);

		return maximumIntegerValue;
	}

	Optional<String> getMinimumIntegerValue(String catalogName, String columnName) {
		Optional<String> minimumIntegerValue = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				minimumIntegerValue = columnMetaData.getMinimumIntegerValue(catalogName, tableName);

		return minimumIntegerValue;
	}

	Optional<String> getMaximumDateTimeValue(String catalogName, String columnName) {
		Optional<String> maximumDateTimeValue = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				maximumDateTimeValue = columnMetaData.getMaximumDateTimeValue(catalogName, tableName);

		return maximumDateTimeValue;
	}

	Optional<String> getMinimumDateTimeValue(String catalogName, String columnName) {
		Optional<String> minimumDateTimeValue = Optional.empty();

		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.getColumnName().equals(columnName))
				minimumDateTimeValue = columnMetaData.getMinimumDateTimeValue(catalogName, tableName);

		return minimumDateTimeValue;
	}

    String getDefaultValue(String columnName) {
        String defaultValue = null;

        for (ColumnMetadata columnMetaData: columnMetaDataSet)
            if (columnMetaData.getColumnName().equals(columnName))
                defaultValue = columnMetaData.getDefaultValue();

        return defaultValue;
    }

	Set<String> getUniqueKeys() {
		Set<String> UniqueKeys = new CopyOnWriteArraySet<>();
		
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.isUniqueKey())
				UniqueKeys.add(columnMetaData.getColumnName());
			
		return UniqueKeys;
	}
	
	Set<String> getForeignKeys() {
		Set<String> foreignKeys = new CopyOnWriteArraySet<>();
		
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			if (columnMetaData.isForeignKey())
				foreignKeys.add(columnMetaData.getColumnName());
			
		return foreignKeys;
	}
	
	List<String> getColumns() {
		List<String> columns = new Vector<>();
		
		for (ColumnMetadata columnMetaData: columnMetaDataSet)
			columns.add(columnMetaData.getColumnName());
			
		return columns;
	}
	
	String getTableName() {
		return tableName;
	}
	
	void addColumnMetaData(ColumnMetadata columnMetaData) {
		columnMetaDataSet.add(columnMetaData);
	}
	
	String getSuperTableName() {
		return superTableName;
	}

	void setSuperTableName(String superTable) {
		this.superTableName = superTable;
	}

	@Override
	public int compareTo(TableMetadata o) {
		return tableName.compareTo(o.getTableName());
	}
}
