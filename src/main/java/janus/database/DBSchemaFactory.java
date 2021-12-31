package janus.database;

import shaper.Shaper;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DBSchemaFactory {
	public static DBSchema generateLocalDatabaseMetaData(DBBridge dbBridge) {
		String catalog = dbBridge.getConnectedCatalog();
		DBSchema localDBMD = new DBSchema(catalog);
		
		Set<String> tables = dbBridge.getTables(catalog);
		for(String table : tables) {
			
			TableMetadata tableMetadata = new TableMetadata(table);

			// primary key
			List<String> primaryKey = dbBridge.getPrimaryKey(catalog, table);
			tableMetadata.setPrimaryKey(primaryKey);

            Set<String> foreignKeys = new CopyOnWriteArraySet<>();

			// referential constraints
            Set<String> refConstraints = dbBridge.getReferentialConstraints(catalog, table);

            for (String refConstraint: refConstraints) {
                RefConstraintMetadata refConstraintMetadata = new RefConstraintMetadata(table, refConstraint);

                String referencedTable = dbBridge.getReferencedTable(catalog, table, refConstraint);
                refConstraintMetadata.setReferencedTableName(referencedTable);

                Set<String> referencingColumns = dbBridge.getReferencingColumns(catalog, table, refConstraint);
                foreignKeys.addAll(referencingColumns);
                for (String referencingColumn: referencingColumns) {
                    short ordinalPosition = dbBridge.getOrdinalPositionInTheRefConstraint(catalog, table, refConstraint, referencingColumn);
                    refConstraintMetadata.addReferencingColumnWithOrdinalPosition(ordinalPosition, referencingColumn);

                    String referencedColumn = dbBridge.getColumnReferencedBy(catalog, table, refConstraint, referencingColumn);
                    refConstraintMetadata.addReferencingColumnAndReferencedColumnPair(referencingColumn, referencedColumn);
                }

                tableMetadata.addReferentialConstraint(refConstraintMetadata);
            }

			Set<String> uniqueKeys = new CopyOnWriteArraySet<>();
            Set<String> singleColumnUniqueKeys = new CopyOnWriteArraySet<>();

            // unique constraints
            Set<String> uniqueConstraints = dbBridge.getUniqueConstraints(catalog, table);

            for (String uniqueConstraint: uniqueConstraints) {
                UniqueConstraintMetadata uniqueConstraintMetadata = new UniqueConstraintMetadata(table, uniqueConstraint);

                Set<String> columnsInTheUniqueConstraint = dbBridge.getUniqueConstraintColumns(catalog, table, uniqueConstraint);

                uniqueKeys.addAll(columnsInTheUniqueConstraint);

                if (columnsInTheUniqueConstraint.size() == 1)
                    singleColumnUniqueKeys.addAll(columnsInTheUniqueConstraint);

                for (String column: columnsInTheUniqueConstraint) {
                    short ordinalPosition = dbBridge.getOrdinalPositionInTheUniqueConstraint(catalog, table, uniqueConstraint, column);
                    uniqueConstraintMetadata.addColumnWithOrdinalPosition(ordinalPosition, column);
                }

                tableMetadata.addUniqueConstraint(uniqueConstraintMetadata);
            }

            // not null constraints
			Set<String> notNullColumns = dbBridge.getNotNullColumns(catalog, table);

            // whether a column is autoincrement
            Set<String> autoIncrementColumns = dbBridge.getAutoIncrementColumns(catalog, table);
			
			String superTable = getSuperTable(primaryKey, foreignKeys, catalog, table);
			tableMetadata.setSuperTableName(superTable);

            List<String> columns = dbBridge.getColumns(catalog, table);

			for(String column: columns) {
				ColumnMetadata columnMetadata = new ColumnMetadata(column);
				
				int dataType = dbBridge.getJDBCDataType(catalog, table, column);
				columnMetadata.setJDBCDataType(dataType);

				String typeName = dbBridge.getColumnType(catalog, table, column);
				columnMetadata.setColumnType(typeName);

                String defaultValue = dbBridge.getDefaultValue(catalog, table, column);
                columnMetadata.setDefaultValue(defaultValue);
				
				if (primaryKey.contains(column))
					columnMetadata.setPrimaryKey(true);
				
				if (foreignKeys.contains(column)) {
					columnMetadata.setForeignKey(true);
					
					String superColumn = dbBridge.getColumnReferencedBy(catalog, table, column);
					columnMetadata.setSuperColumn(superColumn);
				}
				
				if (uniqueKeys.contains(column))
					columnMetadata.setUniqueKey(true);
				
				if (singleColumnUniqueKeys.contains(column))
					columnMetadata.setSingleColumnUniqueKey(true);
				
				if (notNullColumns.contains(column))
					columnMetadata.setNotNull(true);

				if (autoIncrementColumns.contains(column))
				    columnMetadata.setAutoIncrement(true);
				
				tableMetadata.addColumnMetaData(columnMetadata);
			}

			localDBMD.addTableMetaData(tableMetadata);
		}
		
		return localDBMD;
	}
	
	private static String getSuperTable(List<String> pks, Set<String> fks, String catalog, String table) {
		if (fks.containsAll(pks)) {
			Set<String> referencedTables = new CopyOnWriteArraySet<>();
			Set<String> referencedColumns = new CopyOnWriteArraySet<>();
			
			for (String pk: pks) {
				String tableDotColumn = Shaper.dbBridge.getColumnReferencedBy(catalog, table, pk);
				String[] splitTableDotColumn = tableDotColumn.split("\\.");
				String referencedTable = splitTableDotColumn[0];
				String referencedColumn = splitTableDotColumn[1];
				referencedTables.add(referencedTable);
				referencedColumns.add(referencedColumn);
			}
			
			if (referencedTables.size() == 1 && referencedColumns.size() == pks.size()) {
				String superTable = null;
				for (String referencedTable: referencedTables)
					superTable = referencedTable;
				
				List<String> primaryKeysInSuperTable = Shaper.dbBridge.getPrimaryKey(catalog, superTable);
				if (primaryKeysInSuperTable.size() == referencedColumns.size() && primaryKeysInSuperTable.containsAll(referencedColumns))
					return superTable;
			}
		}
		
		return null;
	}
}
