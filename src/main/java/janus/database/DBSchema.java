package janus.database;

import shaper.Shaper;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DBSchema {
	private String catalog;

	private Set<TableMetadata> tables;
	
	DBSchema(String catalog) {
		this.catalog = catalog;
		tables = new CopyOnWriteArraySet<>();
	}

	public String getRegexForXSDDate() {
		return Shaper.dbBridge.getRegexForXSDDate();
	}

	public Optional<String> getRegexForXSDDateTime(String tableName, String columnName) {
		return Shaper.dbBridge.getRegexForXSDDateTime(catalog, tableName, columnName);
	}

	public Optional<String> getRegexForXSDDecimal(String tableName, String columnName) {
		return Shaper.dbBridge.getRegexForXSDDecimal(catalog, tableName, columnName);
	}

	public Optional<String> getRegexForXSDTime(String tableName, String columnName) {
		return Shaper.dbBridge.getRegexForXSDTime(catalog, tableName, columnName);
	}


	public Set<String> getRefConstraints(String referencingTableName) {
	    for (TableMetadata table: tables)
            if (table.getTableName().equals(referencingTableName))
                return table.getRefConstraints();

        return null;
    }

	public Set<DBRefConstraint> getRefConstraintsPointingTo(String referencedTableName) {
		Set<DBRefConstraint> set = new CopyOnWriteArraySet<>();
		for (TableMetadata table: tables) {
			Set<String> refConstraints = table.getRefConstraints();
			for (String refConstraint: refConstraints) {
				if (table.getReferencedTableBy(refConstraint).equals(referencedTableName))
					set.add(new DBRefConstraint(table.getTableName(), refConstraint));
			}
		}

		return set;
	}

    public List<String> getReferencingColumnsByOrdinalPosition(String tableName, String refConstraintName) {
        for (TableMetadata table: tables)
            if (table.getTableName().equals(tableName))
                return table.getReferencingColumnsByOrdinalPosition(refConstraintName);

        return null;
    }

	public String getReferencedColumnBy(String tableName, String refConstraintName, String referencingColumnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getReferencedColumnsBy(refConstraintName, referencingColumnName);

		return null;
	}

    public String getReferencedTableBy(String tableName, String refConstraintName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getReferencedTableBy(refConstraintName);

		return null;
	}
	
	public String getSuperColumn(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getSuperColumn(columnName);
		
		return null;
	}
	
	public String getRootTableDotColumn(String tableName, String columnName) {
		String superColumn = getSuperColumn(tableName, columnName);
		
		while (superColumn != null) {
			String[] tableDotColumn = superColumn.split("\\.");
			tableName = tableDotColumn[0];
			columnName = tableDotColumn[1];
			
			superColumn = getSuperColumn(tableName, columnName);
		}
		
		return tableName + "." + columnName;
	}
	
	public DBColumn getRootColumn(String tableName, String columnName) {
		String superColumn = getSuperColumn(tableName, columnName);
		
		while (superColumn != null) {
			String[] tableDotColumn = superColumn.split("\\.");
			tableName = tableDotColumn[0];
			columnName = tableDotColumn[1];
			
			superColumn = getSuperColumn(tableName, columnName);
		}
		
		return new DBColumn(tableName, columnName);
	}
	
	public boolean isForeignKey(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.isForeignKey(columnName);
		
		return false;
	}

    public boolean isAutoIncrement(String tableName, String columnName) {
        for (TableMetadata table: tables)
            if (table.getTableName().equals(tableName))
                return table.isAutoIncrement(columnName);

        return false;
    }
	
	public boolean isRootTable(String tableName) {
        return getRootTable(tableName).equals(tableName);
	}
	
	public boolean isRootColumn(String tableName, String columnName) {
		String rootColumn = getRootTableDotColumn(tableName, columnName);
		String myColumn = tableName + "." + columnName;

        return rootColumn.equals(myColumn);
	}
	
	public boolean isSingleColumnUniqueKey(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.isSingleColumnUniqueKey(columnName);
		
		return false;
	}
	
	public boolean isPrimaryKey(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.isPrimaryKey(columnName);
		
		return false;
	}
	
	public boolean isNotNull(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.isNotNull(columnName);
		
		return false;
	}
	
	public boolean isKey(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.isKey(columnName);
		
		return false;
	}
	
	public int getJDBCDataType(String tableName, String columnName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getJDBCDataType(columnName);
		
		return 2012;
	}

	public String getColumnType(String tableName, String columnName) {
	    String typeName = null;

	    for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				typeName = table.getColumnType(columnName);

		return typeName;
	}

	public Optional<Set<String>> getValueSet(String tableName, String columnName) {
		Optional<Set<String>> valueSet = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				valueSet = table.getValueSet(catalog, columnName);

		return valueSet;
	}

	public boolean isUnsigned(String tableName, String columnName) {
		boolean isUnsigned = false;

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				isUnsigned = table.isUnsigned(catalog, columnName);

		return isUnsigned;
	}

	public Optional<Integer> getCharacterMaximumLength(String tableName, String columnName) {
		Optional<Integer> characterMaximumLength = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				characterMaximumLength = table.getCharacterMaximumLength(catalog, columnName);

		return characterMaximumLength;
	}

	public Optional<Integer> getMaximumOctetLength(String tableName, String columnName) {
		Optional<Integer> maximumOctetLength = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				maximumOctetLength = table.getMaximumOctetLength(catalog, columnName);

		return maximumOctetLength;
	}

	public Optional<Integer> getNumericPrecision(String tableName, String columnName) {
		Optional<Integer> numericPrecision = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				numericPrecision = table.getNumericPrecision(catalog, columnName);

		return numericPrecision;
	}

	public Optional<Integer> getNumericScale(String tableName, String columnName) {
		Optional<Integer> numericScale = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				numericScale = table.getNumericScale(catalog, columnName);

		return numericScale;
	}

	public Optional<String> getMaximumIntegerValue(String tableName, String columnName) {
		Optional<String> maximumIntegerValue = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				maximumIntegerValue = table.getMaximumIntegerValue(catalog, columnName);

		return maximumIntegerValue;
	}

	public Optional<String> getMinimumIntegerValue(String tableName, String columnName) {
		Optional<String> minimumIntegerValue = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				minimumIntegerValue = table.getMinimumIntegerValue(catalog, columnName);

		return minimumIntegerValue;
	}

	public Optional<String> getMaximumDateTimeValue(String tableName, String columnName) {
		Optional<String> maximumDateTimeValue = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				maximumDateTimeValue = table.getMaximumDateTimeValue(catalog, columnName);

		return maximumDateTimeValue;
	}

	public Optional<String> getMinimumDateTimeValue(String tableName, String columnName) {
		Optional<String> minimumDateTimeValue = Optional.empty();

		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				minimumDateTimeValue = table.getMinimumDateTimeValue(catalog, columnName);

		return minimumDateTimeValue;
	}

	public Optional<String> getMaximumDateValue() {
		return Shaper.dbBridge.getMaximumDateValue();
	}

	public Optional<String> getMinimumDateValue() {
		return Shaper.dbBridge.getMinimumDateValue();
	}

    public String getDefaultValue(String tableName, String columnName) {
        String defaultValue = null;

        for (TableMetadata table: tables)
            if (table.getTableName().equals(tableName))
                defaultValue = table.getDefaultValue(columnName);

        return defaultValue;
    }
	
	public void addTableMetaData(TableMetadata tableMetaData) {
		tables.add(tableMetaData);
	}
	
	public String getCatalog() {
		return catalog;
	}
	
	public Set<String> getTableNames() {
		Set<String> tableNames = new CopyOnWriteArraySet<>();
		
		for (TableMetadata table: tables)
			tableNames.add(table.getTableName());
		
		return tableNames;
	}
	
	public List<String> getColumns(String tableName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getColumns();
		
		return null;
	}
	
	public List<String> getPrimaryKey(String tableName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getPrimaryKey();
		
		return null;
	}
	
	public boolean isPrimaryKeySingleColumn(String tableName) {
        return getPrimaryKey(tableName).size() < 2;
    }
	
	public Set<String> getForeignKeys(String tableName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getForeignKeys();
		
		return null;
	}
	
	public Set<String> getUniqueKeys(String tableName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getUniqueKeys();
		
		return null;
	}
	
	public String getSuperTable(String tableName) {
		for (TableMetadata table: tables)
			if (table.getTableName().equals(tableName))
				return table.getSuperTableName();
		
		return null;
	}
	
	public String getRootTable(String tableName) {
		String superTable = getSuperTable(tableName);
		
		while (superTable != null) {
			tableName = superTable;
			
			superTable = getSuperTable(tableName);
		}
		
		return tableName;
	}
	
	public Set<String> getKeyColumns(String tableName) {
		Set<String> keyColumns = new CopyOnWriteArraySet<>();
		
		keyColumns.addAll(getPrimaryKey(tableName));
		keyColumns.addAll(getForeignKeys(tableName));
		keyColumns.addAll(getUniqueKeys(tableName));
		
		return keyColumns;
	}
	
	public Set<String> getNonKeyColumns(String tableName) {
		Set<String> nonKeyColumns = new CopyOnWriteArraySet<>();
		
		nonKeyColumns.addAll(getColumns(tableName));
		nonKeyColumns.removeAll(getKeyColumns(tableName));
		
		return nonKeyColumns;
	}
	
	public String getMatchedPKColumnAmongFamilyTables(String srcTable, String srcColumn, String targetTable) {
		String rootTableName = getRootTable(targetTable);
		
		String superTable = srcTable;
		String superColumn = srcColumn;
		while (!superTable.equals(rootTableName)) {
			String superTableDotColumn = getSuperColumn(superTable, superColumn);
			superTable = superTableDotColumn.substring(0, superTableDotColumn.indexOf("."));
			superColumn = superTableDotColumn.substring(superTableDotColumn.indexOf(".")+1);
		}
		String matchedRootColumnForMe = superColumn;
		
		List<String> targetPKs = getPrimaryKey(targetTable);
		for (String targetPK: targetPKs) {
			superTable = targetTable;
			superColumn = targetPK;
			while (!superTable.equals(rootTableName)) {
				String superTableDotColumn = getSuperColumn(superTable, superColumn);
				superTable = superTableDotColumn.substring(0, superTableDotColumn.indexOf("."));
				superColumn = superTableDotColumn.substring(superTableDotColumn.indexOf(".")+1);
			}
			String matchedRootColumnForTarget = superColumn;
			
			if (matchedRootColumnForTarget.equals(matchedRootColumnForMe))
				return targetPK;
		}
		
		return null;
	}
	
	public Set<String> getFamilyTables(String tableName) {
		Set<String> familyTables = new CopyOnWriteArraySet<>();
		
		String rootTable = getRootTable(tableName);
		
		Set<String> allTables = getTableNames();
		for (String table: allTables)
			if (getRootTable(table).equals(rootTable))
				familyTables.add(table);
		
		return familyTables;
	}
	
	public Set<DBColumn> getFamilyColumns(DBColumn column) {
		Set<DBColumn> familyColumns = new CopyOnWriteArraySet<>();
		
		DBColumn rootColumn = getRootColumn(column.getTableName(), column.getColumnName());
		
		Set<String> allTables = getTableNames();
		for (String table: allTables) {
			Set<String> columns = getKeyColumns(table);
			for (String aColumn: columns)
				if (getRootColumn(table, aColumn).equals(rootColumn))
					familyColumns.add(new DBColumn(table, aColumn));
		}
		
		return familyColumns;
	}
	
	public Set<DBColumn> getFamilyColumns(String tableName, String columnName) {
		return getFamilyColumns(new DBColumn(tableName, columnName));
	}
	
	public Set<DBColumn> getColumsIncludedInTables(Set<String> tables, Set<DBColumn> columns) {
		Set<DBColumn> members = new CopyOnWriteArraySet<>();
		
		for (DBColumn column: columns)
			if (tables.contains(column.getTableName()))
				members.add(column);
		
		return members;
	}
	
	public Set<String> getRootTables() {
		Set<String> rootTables = new CopyOnWriteArraySet<>();
		
		Set<String> tables = getTableNames();
		for (String table: tables)
			if (isRootTable(table))
				rootTables.add(table);
		
		return rootTables;
	}
	
	public Set<DBColumn> getRootColumns() {
		Set<DBColumn> rootColumns = new CopyOnWriteArraySet<>();
		
		Set<String> tables = getTableNames();
		for (String table: tables) {
			Set<String> keyColumns = getKeyColumns(table);
			for (String column: keyColumns)
				if (isRootColumn(table, column))
					rootColumns.add(new DBColumn(table, column));
		}
		
		return rootColumns;
	}
}
