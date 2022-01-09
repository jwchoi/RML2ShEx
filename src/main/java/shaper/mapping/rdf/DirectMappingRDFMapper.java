package shaper.mapping.rdf;

import janus.database.DBField;
import janus.database.DBMSTypes;
import janus.database.SQLResultSet;
import shaper.Shaper;
import shaper.mapping.PrefixMap;
import shaper.mapping.Symbols;
import shaper.mapping.model.dm.DMModelFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class DirectMappingRDFMapper extends RDFMapper {

	private void preProcess(Extension extension) {
		String catalog = Shaper.dbSchema.getCatalog();

		switch (extension) {
			case Turtle:
				output = new File(Shaper.DEFAULT_DIR_FOR_RDF_FILE + catalog + "." + extension);
				break;
		}
		try {
			writer = new PrintWriter(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void writeDirectives(Extension extension) {
		try {
			switch (extension) {
				case Turtle:
					// base
					writer.println(Symbols.AT + Symbols.base + Symbols.SPACE + Symbols.LT + Shaper.rdfBaseURI + Symbols.GT + Symbols.SPACE + Symbols.DOT);

					// prefixID
					writer.println(Symbols.AT + Symbols.prefix + Symbols.SPACE + "rdf" + Symbols.COLON + Symbols.SPACE + Symbols.LT + PrefixMap.getURI("rdf") + Symbols.GT + Symbols.SPACE + Symbols.DOT);
					writer.println(Symbols.AT + Symbols.prefix + Symbols.SPACE + "xsd" + Symbols.COLON + Symbols.SPACE + Symbols.LT + PrefixMap.getURI("xsd") + Symbols.GT + Symbols.SPACE + Symbols.DOT);
					writer.println(Symbols.AT + Symbols.prefix + Symbols.SPACE + "rdfs" + Symbols.COLON + Symbols.SPACE + Symbols.LT + PrefixMap.getURI("rdfs") + Symbols.GT + Symbols.SPACE + Symbols.DOT);
					writer.println();
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void postProcess() {
		try {
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void writeRDF() {
		Set<String> tableNames = Shaper.dbSchema.getTableNames();
		
		for (String tableName : tableNames) {
			String tableIRI = dmModel.getMappedTableIRILocalPart(tableName);
			
			String query = getQueryFor(tableName, Shaper.DBMSType);
			SQLResultSet resultSet = Shaper.dbBridge.executeQuery(query);
			
			int columnCount = resultSet.getResultSetColumnCount();
			
			List<String> columnNames = new Vector<>(columnCount);
			for(int column = 1; column <= columnCount; column++) {
				String columnName = resultSet.getResultSetColumnLabel(column);
				columnNames.add(columnName);
			}

			List<String> primaryKeys = Shaper.dbSchema.getPrimaryKey(tableName);

			int rowCount = resultSet.getResultSetRowCount();
			for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
				List<String> rowData = resultSet.getResultSetRowAt(rowIndex);

				StringBuffer buffer = new StringBuffer();

				String subject;

				if (!primaryKeys.isEmpty()) {
                    List<DBField> pkFields = new Vector<>();
                    for (String pk : primaryKeys) {
                        int pkIndex = columnNames.indexOf(pk);
                        String pkData = rowData.get(pkIndex);
                        DBField pkField = new DBField(tableName, pk, pkData);

                        pkFields.add(pkField);
                    }

                    subject = Symbols.LT + dmModel.getMappedRowNode(tableName, pkFields) + Symbols.GT;
                } else {
                    List<DBField> fields = new Vector<>();
                    for (String columnName : columnNames) {
                        int index = columnNames.indexOf(columnName);
                        String fieldData = rowData.get(index);
                        DBField field = new DBField(tableName, columnName, fieldData);

                        fields.add(field);
                    }

                    subject = dmModel.getMappedBlankNode(tableName, fields);
                }

                buffer.append(subject + Symbols.NEWLINE);
				buffer.append(Symbols.TAB + Symbols.A + Symbols.SPACE + Symbols.LT + tableIRI + Symbols.GT + Symbols.SPACE + Symbols.SEMICOLON + Symbols.NEWLINE);

				for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
					String cellData = rowData.get(columnIndex);
					
					if (cellData == null) continue;
					
					String columnName = columnNames.get(columnIndex);
					String literalProperty = dmModel.getMappedLiteralProperty(tableName, columnName);

					String literal = dmModel.getMappedLiteral(tableName, columnName, cellData);

					buffer.append(Symbols.TAB + Symbols.LT + literalProperty + Symbols.GT + Symbols.SPACE + literal + Symbols.SPACE + Symbols.SEMICOLON + Symbols.NEWLINE);
				}

				int lastSemicolon = buffer.lastIndexOf(Symbols.SEMICOLON);
				buffer.replace(lastSemicolon, lastSemicolon+1, Symbols.DOT);
				writer.println(buffer);
			}
		}

		for (String tableName : tableNames) {

			String referencingTable = tableName;

			List<String> pkOfReferencingTable = Shaper.dbSchema.getPrimaryKey(referencingTable);
			List<String> columnsOfReferencingTable = Shaper.dbSchema.getColumns(referencingTable);

			Set<String> refConstraints = Shaper.dbSchema.getRefConstraints(tableName);

			for (String refConstraint: refConstraints) {

				String referenceProperty = dmModel.getMappedReferenceProperty(referencingTable, refConstraint);

				String referencedTable = Shaper.dbSchema.getReferencedTableBy(referencingTable, refConstraint);
				List<String> pkOfReferencedTable = Shaper.dbSchema.getPrimaryKey(referencedTable);
				List<String> columnsOfReferencedTable = Shaper.dbSchema.getColumns(referencedTable);

				NaturalJoinQuery query = getNaturalJoinQueryFor(tableName, refConstraint);
				SQLResultSet resultSet = Shaper.dbBridge.executeQuery(query.toString());

				int rowCount = resultSet.getResultSetRowCount();
				for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
					List<String> rowData = resultSet.getResultSetRowAt(rowIndex);

					String subject;

					if (!pkOfReferencingTable.isEmpty()) {
						List<DBField> pkFields = new Vector<>();
						for (String pk : pkOfReferencingTable) {
							int pkIndex = query.getSelectColumnIndexOf(referencingTable, query.getAliasOfReferencingTable(), pk);
							String pkData = rowData.get(pkIndex);
							DBField pkField = new DBField(referencingTable, pk, pkData);

							pkFields.add(pkField);
						}


						subject = Symbols.LT + dmModel.getMappedRowNode(referencingTable, pkFields) + Symbols.GT;
					} else {
						List<DBField> fields = new Vector<>();
						for (String col : columnsOfReferencingTable) {
							int index = query.getSelectColumnIndexOf(referencingTable, query.getAliasOfReferencingTable(), col);
							String fieldData = rowData.get(index);
							DBField field = new DBField(referencingTable, col, fieldData);

							fields.add(field);
						}

						subject = dmModel.getMappedBlankNode(referencingTable, fields);
					}

					String object;

					if (!pkOfReferencedTable.isEmpty()) {
						List<DBField> pkFields = new Vector<>();
						for (String pk : pkOfReferencedTable) {
							int pkIndex = query.getSelectColumnIndexOf(referencedTable, query.getAliasOfReferencedTable(), pk);
							String pkData = rowData.get(pkIndex);
							DBField pkField = new DBField(referencedTable, pk, pkData);

							pkFields.add(pkField);
						}

						object = Symbols.LT + dmModel.getMappedRowNode(referencedTable, pkFields) + Symbols.GT;
					} else {
						List<DBField> fields = new Vector<>();
						for (String col : columnsOfReferencedTable) {
							int index = query.getSelectColumnIndexOf(referencedTable, query.getAliasOfReferencedTable(), col);
							String fieldData = rowData.get(index);
							DBField field = new DBField(referencedTable, col, fieldData);

							fields.add(field);
						}

						object = dmModel.getMappedBlankNode(referencedTable, fields);
					}

					writer.println(subject + Symbols.SPACE + Symbols.LT + referenceProperty + Symbols.GT + Symbols.SPACE + object + Symbols.SPACE + Symbols.DOT);
				}
			}
		}
	}
	
	public File generateRDFFile() {
		dmModel = DMModelFactory.generateMappingModel(Shaper.dbSchema);

		preProcess(Extension.Turtle);
		writeDirectives(Extension.Turtle);
		writeRDF();
		postProcess();
		
		System.out.println("The Direct Mapping has finished.");
		
		return output;
	}

	private String getQueryFor(String tableName, DBMSTypes dbmsType) {
		if (tableName.contains(Symbols.SPACE)) {
			switch (dbmsType) {
				case MARIADB:
				case MYSQL:
					tableName = Symbols.GRAVE_ACCENT + tableName + Symbols.GRAVE_ACCENT;
					break;
			}
		}

		return "SELECT * FROM " + tableName;
	}

	private NaturalJoinQuery getNaturalJoinQueryFor(String tableName, String refConstraint) {
		String referencingTable = tableName;
		String aliasOfReferencingTable = "T1";

		String referencedTable = Shaper.dbSchema.getReferencedTableBy(referencingTable, refConstraint);
		String aliasOfReferencedTable = "T2";

		NaturalJoinQuery naturalJoinQuery = new NaturalJoinQuery(referencingTable, aliasOfReferencingTable, referencedTable, aliasOfReferencedTable);

		StringBuffer query = new StringBuffer("SELECT ");

		List<String> pkOfReferencingTable = Shaper.dbSchema.getPrimaryKey(referencingTable);
		if (!pkOfReferencingTable.isEmpty()) {
			for (String column: pkOfReferencingTable) {
				naturalJoinQuery.addSQLSelectColumn(referencingTable, aliasOfReferencingTable, column);
				query.append(aliasOfReferencingTable + "." + column);
				query.append(", ");
			}
		} else {
			List<String> columns = Shaper.dbSchema.getColumns(referencingTable);
			for (String column: columns) {
				naturalJoinQuery.addSQLSelectColumn(referencingTable, aliasOfReferencingTable, column);
				query.append(aliasOfReferencingTable + "." + column);
				query.append(", ");
			}
		}

		List<String> pkOfReferencedTable = Shaper.dbSchema.getPrimaryKey(referencedTable);
		if (!pkOfReferencedTable.isEmpty()) {
			for (String column: pkOfReferencedTable) {
				naturalJoinQuery.addSQLSelectColumn(referencedTable, aliasOfReferencedTable, column);
				query.append(aliasOfReferencedTable + "." + column);
				query.append(", ");
			}
		} else {
			List<String> columns = Shaper.dbSchema.getColumns(referencedTable);
			for (String column: columns) {
				naturalJoinQuery.addSQLSelectColumn(referencedTable, aliasOfReferencedTable, column);
				query.append(aliasOfReferencedTable + "." + column);
				query.append(", ");
			}
		}

		query.deleteCharAt(query.lastIndexOf(","));

		query.append("FROM " + referencingTable + " AS " + aliasOfReferencingTable + ", " + referencedTable + " AS " + aliasOfReferencedTable);
		query.append(" WHERE ");

		List<String> referencingColumns = Shaper.dbSchema.getReferencingColumnsByOrdinalPosition(referencingTable, refConstraint);
		for (String referencingColumn: referencingColumns) {
			String referencedColumn = Shaper.dbSchema.getReferencedColumnBy(referencingTable, refConstraint, referencingColumn);
			query.append(aliasOfReferencingTable + "." + referencingColumn + " = " + aliasOfReferencedTable + "." + referencedColumn + " AND ");
		}

		query.delete(query.lastIndexOf(" AND "), query.length());

		naturalJoinQuery.setQuery(query.toString());

		return naturalJoinQuery;
    }

    private class NaturalJoinQuery {

		class SQLSelectColumn {
			private String tableName;
			private String columnName;

			private String aliasOfTableName;

			SQLSelectColumn(String tableName, String columnName, String aliasOfTableName) {
				this.tableName = tableName;
				this.columnName = columnName;
				this.aliasOfTableName = aliasOfTableName;
			}

			String getTableName() {
				return tableName;
			}

			String getColumnName() {
				return columnName;
			}

			String getAliasOfTableName() {
				return aliasOfTableName;
			}
		}

		private String query;

		private String referencingTable;
		private String referencedTable;

		private String aliasOfReferencingTable;
		private String aliasOfReferencedTable;

		private List<SQLSelectColumn> selectColumns;

		NaturalJoinQuery(String referencingTable, String aliasOfReferencingTable, String referencedTable, String aliasOfReferencedTable) {
			this.referencingTable = referencingTable;
			this.aliasOfReferencingTable = aliasOfReferencingTable;

			this.referencedTable = referencedTable;
			this.aliasOfReferencedTable = aliasOfReferencedTable;

			selectColumns = new ArrayList<>();
		}

		void addSQLSelectColumn(String tableName, String aliasOfTableName, String columnName) {
			selectColumns.add(new SQLSelectColumn(tableName, columnName, aliasOfTableName));
		}

		int getSelectColumnIndexOf(String tableName, String aliasOfTableName, String columnName) {
			for (int i = 0; i < selectColumns.size(); i++) {
				SQLSelectColumn selectColumn = selectColumns.get(i);
				if (selectColumn.getTableName().equals(tableName)
						&& selectColumn.getAliasOfTableName().equals(aliasOfTableName)
						&& selectColumn.getColumnName().equals(columnName))
					return i;
			}

			return -1;
		}

		void setQuery(String query) { this.query = query; }

		@Override
		public String toString() {
			return query;
		}

		String getAliasOfReferencingTable() {
			return aliasOfReferencingTable;
		}

		String getAliasOfReferencedTable() {
			return aliasOfReferencedTable;
		}
	}
}
