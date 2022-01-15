package rml2shex.datasource.db;

import java.sql.*;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public abstract class DBBridge {
	Connection connection;
	
	void loadDriver(String driver) {
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	Connection getConnection(String url, String user, String password) {
		Connection connection = null;
		
		try {
			connection = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) { e.printStackTrace(); }

		return connection;
	}
	
	boolean isConnected() {
		return connection != null;
	}
	
	public void disconnect() {
		try {
			if (isConnected()) connection.close();
		} catch (SQLException e) { e.printStackTrace(); }
	}

	// example: varchar(50)
	String getColumnType(String catalog, String table, String column) {
		String typeName = null;
		
		try {
			DatabaseMetaData dbmd = connection.getMetaData();
			
			ResultSet columns = dbmd.getColumns(catalog, null, table, column);
			columns.absolute(1);
			typeName = columns.getString("TYPE_NAME");
		} catch(SQLException e) { e.printStackTrace(); }

		return typeName;
	}
	
	int getJDBCDataType(String catalog, String table, String column) {
		int type = 2012;
		
		try {
			DatabaseMetaData dbmd = connection.getMetaData();
			
			ResultSet columns = dbmd.getColumns(catalog, null, table, column);
			columns.absolute(1);
			type = columns.getInt("DATA_TYPE");
		} catch(SQLException e) { e.printStackTrace(); }
		
		return type;
	}

	// example: varchar
	abstract String getSQLDataType(String catalog, String table, String column);
	
	String getDefaultValue(String catalog, String table, String column) {
		String defaultValue = null;
		
		try {
			DatabaseMetaData dbmd = connection.getMetaData();
			
			ResultSet columns = dbmd.getColumns(catalog, null, table, column);
			columns.absolute(1);
			defaultValue = columns.getString("COLUMN_DEF");
		} catch(SQLException e) { e.printStackTrace(); }
		
		return defaultValue;
	}

	abstract Optional<Integer> getCharacterMaximumLength(String catalog, String table, String column);

	abstract Optional<Integer> getCharacterOctetLength(String catalog, String table, String column);

	abstract Optional<Integer> getNumericPrecision(String catalog, String table, String column);

	abstract Optional<Integer> getNumericScale(String catalog, String table, String column);

	abstract Optional<Set<String>> getValueSet(String catalog, String table, String column);

	abstract boolean isUnsigned(String catalog, String table, String column);

	abstract Optional<String> getMaximumIntegerValue(String catalog, String table, String column);

    abstract Optional<String> getMinimumIntegerValue(String catalog, String table, String column);

	abstract Optional<String> getMaximumDateTimeValue(String catalog, String table, String column);

	abstract Optional<String> getMinimumDateTimeValue(String catalog, String table, String column);

	abstract Optional<String> getMaximumDateValue();

	abstract Optional<String> getMinimumDateValue();

	abstract String getRegexForXSDDate();

	abstract Optional<String> getRegexForXSDDateTime(String catalog, String table, String column);

	abstract Optional<String> getRegexForXSDTime(String catalog, String table, String column);

	abstract Optional<String> getRegexForXSDDecimal(String catalog, String table, String column);
	
	// about DB Scheme
    String getConnectedCatalog() {
        String db = null;

        try {
            db = connection.getCatalog();
        } catch(SQLException e) { e.printStackTrace(); }

        return db;
    }

    Set<String> getTables(String catalog) {
        Set<String> set = new CopyOnWriteArraySet<>();

        try {
            DatabaseMetaData dbmd = connection.getMetaData();

            String[] types = {"TABLE"/*, "VIEW", "SYSTEM TABLE"/*, "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"*/};
            ResultSet tableSet = dbmd.getTables(catalog, null, null, types);
            while (tableSet.next())
                set.add(tableSet.getString("TABLE_NAME"));
        } catch(SQLException e) { e.printStackTrace(); }

        return set;
    }
	
	// ordered by KEY_SEQ
	List<String> getPrimaryKey(String catalog, String table) {
		List<String> pks = new Vector<>();
		
		Map<Short, String> pkMap = new Hashtable<>();
		
		try {
			DatabaseMetaData dbmd = connection.getMetaData();
			
			ResultSet pkSet = dbmd.getPrimaryKeys(catalog, null, table);
			
			while (pkSet.next())
				pkMap.put(pkSet.getShort("KEY_SEQ"), pkSet.getString("COLUMN_NAME"));
			
			for (short i = 1; i <= pkMap.size(); i++)
				pks.add(pkMap.get(i));
			
		} catch(SQLException e) { e.printStackTrace(); }
		
		return pks;
	}
	
	// ordered by ORDINAL_POSITION
	List<String> getColumns(String catalog, String table) {
		List<String> cols = new Vector<>();

		try {
			DatabaseMetaData dbmd = connection.getMetaData();

			ResultSet fieldSet = dbmd.getColumns(catalog, null, table, null);

			while (fieldSet.next()) {
				String fieldName = fieldSet.getString("COLUMN_NAME");
				cols.add(fieldName);
			}
		} catch(SQLException e) { e.printStackTrace(); }

		return cols;
	}

	// for ref-constraint
	abstract Set<String> getReferentialConstraints(String catalog, String table);
	abstract Set<String> getReferencingColumns(String catalog, String table, String refConstraint);
	abstract short getOrdinalPositionInTheRefConstraint(String catalog, String table, String refConstraint, String column);
	abstract String getReferencedTable(String catalog, String table, String refConstraint);
    abstract String getColumnReferencedBy(String catalog, String table, String refConstraint, String column);

    // for unique-constraint
    abstract Set<String> getUniqueConstraints(String catalog, String table);
    abstract Set<String> getUniqueConstraintColumns(String catalog, String table, String uniqueConstraint);
    abstract short getOrdinalPositionInTheUniqueConstraint(String catalog, String table, String uniqueConstraint, String column);
	
	Set<String> getNotNullColumns(String catalog, String table) {
		Set<String> set = new CopyOnWriteArraySet<>();

		try {
			DatabaseMetaData dbmd = connection.getMetaData();

			ResultSet fieldSet = dbmd.getColumns(catalog, null, table, null);
			
			while (fieldSet.next()) {
				String isNullable = fieldSet.getString("IS_NULLABLE");
				if (isNullable.equals("NO")) {
					String columnName = fieldSet.getString("COLUMN_NAME");
					set.add(columnName);
				}
			}
		} catch(SQLException e) { e.printStackTrace(); }

		return set;
	}

    Set<String> getAutoIncrementColumns(String catalog, String table) {
        Set<String> set = new CopyOnWriteArraySet<>();
		
		try {
			DatabaseMetaData dbmd = connection.getMetaData();
			
			ResultSet columns = dbmd.getColumns(catalog, null, table, null);

            while (columns.next()) {
                String isAutoincrement = columns.getString("IS_AUTOINCREMENT");
                if (isAutoincrement.equals("YES")) {
                    String columnName = columns.getString("COLUMN_NAME");
                    set.add(columnName);
                }
            }

		} catch(SQLException e) { e.printStackTrace(); }
		
		return set;
	}
	
	public String getColumnReferencedBy(String catalog, String table, String column) {
		try {
			DatabaseMetaData dbmd = connection.getMetaData();

			ResultSet fkSet = dbmd.getImportedKeys(catalog, null, table);
			while (fkSet.next())
				if (column.equals(fkSet.getString("FKCOLUMN_NAME"))) {
					String pkTable = fkSet.getString("PKTABLE_NAME");
					String pkColumn = fkSet.getString("PKCOLUMN_NAME");
					
					return pkTable + "." + pkColumn;
				}
					
		} catch(SQLException e) { e.printStackTrace(); }
		
		return null;
	}
	
	// about SQL query
    public SQLResultSet executeQuery(String query) {
        SQLResultSet SQLRS = null;

        try {
            Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);

            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            SQLRS = new SQLResultSet(rs, rsmd);

        } catch(SQLException e) { e.printStackTrace(); }

        return SQLRS;
    }

	abstract SQLResultSet executeQueryFromInformationSchema(String query);
	
	abstract String buildURL(String host, String port, String schema);

	abstract Optional<ZoneOffset> getDBZoneOffset();

	abstract boolean isTimeZoneAwareDataType(String dbTypeName);
}
