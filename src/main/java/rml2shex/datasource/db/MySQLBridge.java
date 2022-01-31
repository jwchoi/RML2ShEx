package rml2shex.datasource.db;

import rml2shex.commons.SqlXsdMap;
import rml2shex.commons.XSDs;

import java.sql.*;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

final class MySQLBridge extends DBBridge {

    //private final String regexForXSDDate = "^([1-9][0-9]{3})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$";
    private final String regexForXSDDate = "^\\d{4}-\\d{2}-\\d{2}$";

    //private final String defaultRegexForXSDDateTimeFromDateTime = "^([1-9][0-9]{3})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])((\\.[0-9]{1,6})?)$";
    private final String defaultRegexForXSDDateTimeFromDateTime = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.{?}[\\d]{min,max}){?}$";

    //private final String defaultRegexForXSDDateTimeFromTimeStamp = "(^(19[7-9][0-9]|20([0-2][0-9]|3[0-7]))-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])((\\.[0-9]{1,6})?)$)|(^2038-01-(0[1-9]|1[0-8])T(0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])((\\.[0-9]{1,6})?)$)|(^2038-01-19T0[0-2]:([0-5][0-9]):([0-5][0-9])((\\.[0-9]{1,6})?)$)|(^2038-01-19T03:(0[0-9]|1[0-3]):([0-5][0-9])((\\.[0-9]{1,6})?))|(2038-01-19T03:14:0[0-6]((\\.[0-9]{1,6})?)$)|(^2038-01-19T03:14:07((\\.0{1,6})?)$)";
    private final String defaultRegexForXSDDateTimeFromTimeStamp = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.{?}[\\d]{min,max}){?}Z$";

    private final String defaultRegexForXSDTime = "^([01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(\\.\\d{min,max}){?}|(24:00:00(\\.0{min,max}){?})$";

    private final String defaultRegexForXSDDecimal = "^(\\+|-)?\\d{0,p-s}(\\.\\d{0,s})?$";

    private enum IntegerTypes {
        TINYINT_SIGNED_MINIMUM_VALUE("-128"),
        TINYINT_SIGNED_MAXIMUM_VALUE("127"),
        TINYINT_UNSIGNED_MINIMUM_VALUE("0"),
        TINYINT_UNSIGNED_MAXIMUM_VALUE("255"),

        SMALLINT_SIGNED_MINIMUM_VALUE("-32768"),
        SMALLINT_SIGNED_MAXIMUM_VALUE("32767"),
        SMALLINT_UNSIGNED_MINIMUM_VALUE("0"),
        SMALLINT_UNSIGNED_MAXIMUM_VALUE("65535"),

        MEDIUMINT_SIGNED_MINIMUM_VALUE("-8388608"),
        MEDIUMINT_SIGNED_MAXIMUM_VALUE("8388607"),
        MEDIUMINT_UNSIGNED_MINIMUM_VALUE("0"),
        MEDIUMINT_UNSIGNED_MAXIMUM_VALUE("16777215"),

        INT_SIGNED_MINIMUM_VALUE("-2147483648"),
        INT_SIGNED_MAXIMUM_VALUE("2147483647"),
        INT_UNSIGNED_MINIMUM_VALUE("0"),
        INT_UNSIGNED_MAXIMUM_VALUE("4294967295"),

        BIGINT_SIGNED_MINIMUM_VALUE("-9223372036854775808"),
        BIGINT_SIGNED_MAXIMUM_VALUE("9223372036854775807"),
        BIGINT_UNSIGNED_MINIMUM_VALUE("0"),
        BIGINT_UNSIGNED_MAXIMUM_VALUE("18446744073709551615");

        private final String value;

        IntegerTypes(String value) {
            this.value = value;
        }

        @Override
        public String toString() { return value; }
    }

    private enum DateTimeTypes {
        DATETIME_MINIMUM_VALUE("1000-01-01T00:00:00.000000"),
        DATETIME_MAXIMUM_VALUE("9999-12-31T23:59:59.999999");

        private final String value;

        DateTimeTypes(String value) {
            this.value = value;
        }

        @Override
        public String toString() { return value; }
    }

    private enum DateTypes {
        DATE_MINIMUM_VALUE("1000-01-01"),
        DATE_MAXIMUM_VALUE("9999-12-31");

        private final String value;

        DateTypes(String value) {
            this.value = value;
        }

        @Override
        public String toString() { return value; }
    }

    private Connection informationSchemaConnection;

    private String informationSchemaName = "information_schema";

	private MySQLBridge(String host, String port, String id, String password, String schema) {
		loadDriver(DBMSTypes.MYSQL.driver());
		
		connection = getConnection(buildURL(host, port, schema), id, password);

        informationSchemaConnection = getConnection(buildURL(host, port, informationSchemaName), id, password);
	}
	
	static MySQLBridge getInstance(String host, String port, String id, String password, String schema) {
		MySQLBridge dbBridge = new MySQLBridge(host, port, id, password, schema);

		return dbBridge.isConnected() ? dbBridge : null;
	}

    @Override
	String buildURL(String host, String port, String schema) {
		return "jdbc:mysql://" + host + ":" + port + "/" + schema;
	}

    @Override
    boolean isTimeZoneAwareDataType(String dbTypeName) {
        return (dbTypeName.toUpperCase().equals("TIMESTAMP")) ? true : false;
    }

    @Override
    Optional<ZoneOffset> getDBZoneOffset() {
        Optional<ZoneOffset> zoneOffset = Optional.empty();

        try {
            Statement stmt = informationSchemaConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);

            String query = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP)";

            ResultSet rs = stmt.executeQuery(query);
            if (rs.first()) {
                String value = rs.getString(1);
                if (!value.startsWith("-"))
                    value = "+" + value;
                zoneOffset = Optional.of(ZoneOffset.of(value));
            }

        } catch(SQLException e) { e.printStackTrace(); }

        return zoneOffset;
    }

	@Override
    SQLResultSet executeQueryFromInformationSchema(String query) {
        SQLResultSet SQLRS = null;

        try {
            Statement stmt = informationSchemaConnection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);

            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            SQLRS = new SQLResultSet(rs, rsmd);
        } catch(SQLException e) { e.printStackTrace(); }

        return SQLRS;
    }

    @Override
    Optional<String> getRegexForXSDTime(String catalog, String table, String column) {
        String regex = null;

        String SQLDataType = getSQLDataType(catalog, table, column);

        if (SQLDataType.toUpperCase().equals("TIME")) {

            Integer msPrecision = getDateTimePrecision(catalog, table, column).get();

            if (SQLDataType.toUpperCase().equals("TIME")) {
                if (msPrecision == 0)
                    regex = defaultRegexForXSDTime.replace("?", "{0}");
                else if (msPrecision > 0 && msPrecision < 6)
                    regex = defaultRegexForXSDTime.replace("6", msPrecision.toString());
            }
        }

        return Optional.ofNullable(regex);
    }

    @Override
    Optional<String> getRegexForXSDDateTime(String catalog, String table, String column) {
        String regex = null;

        String SQLDataType = getSQLDataType(catalog, table, column);

        if (SQLDataType.toUpperCase().equals("DATETIME") || SQLDataType.toUpperCase().equals("TIMESTAMP")) {

            Integer msPrecision = getDateTimePrecision(catalog, table, column).get();

            regex = defaultRegexForXSDDateTimeFromTimeStamp.replace("max", msPrecision.toString());
            if (msPrecision < 1) {
                regex = regex.replace("min", "0");
                regex = regex.replace("?", "0");
            } else {
                regex = regex.replace("min", "1");
                regex = regex.replace("?", "1");
            }
        }

        return Optional.ofNullable(regex);
    }

    @Override
    Optional<String> getRegexForXSDDecimal(String catalog, String table, String column) {
        String regex = null;

        String SQLDataType = getSQLDataType(catalog, table, column);

        if (SQLDataType.toUpperCase().equals("DECIMAL")) {
            Optional<Integer> numericPrecision = getNumericPrecision(catalog, table, column);
            Optional<Integer> numericScale = getNumericScale(catalog, table, column);

            if (numericPrecision.isPresent() && numericScale.isPresent()) {
                Integer p = numericPrecision.get();
                Integer s = numericScale.get();
                regex = defaultRegexForXSDDecimal.replace("p-s", Integer.toString(p-s));
                regex = regex.replace("s", Integer.toString(s));
            }
        }

        return Optional.ofNullable(regex);
    }

    private Optional<Integer> getDateTimePrecision(String catalog, String table, String column) {
        Integer dateTimePrecision = null;

        String query = "SELECT DATETIME_PRECISION " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        try {
            dateTimePrecision = Integer.valueOf(rowData.get(0));
        } catch (NumberFormatException e) {}

        return Optional.ofNullable(dateTimePrecision);
    }

    @Override
    String getRegexForXSDDate() { return regexForXSDDate; }

    @Override
    Set<String> getUniqueConstraints(String catalog, String table) {
        Set<String> uniqueConstraints = new CopyOnWriteArraySet<>();

        String query = "SELECT CONSTRAINT_NAME " +
                "FROM TABLE_CONSTRAINTS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_TYPE" + " = " + "'" + "UNIQUE" + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        int rowCount = resultSet.getResultSetRowCount();
        for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
            List<String> rowData = resultSet.getResultSetRowAt(rowIndex);
            String constraintName = rowData.get(0);
            uniqueConstraints.add(constraintName);
        }

        return uniqueConstraints;
    }

    @Override
    Set<String> getUniqueConstraintColumns(String catalog, String table, String uniqueConstraint) {
        Set<String> uniqueConstraintColumns = new CopyOnWriteArraySet<>();

        String query = "SELECT COLUMN_NAME " +
                "FROM KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_NAME" + " = " + "'" + uniqueConstraint + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        int rowCount = resultSet.getResultSetRowCount();
        for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
            List<String> rowData = resultSet.getResultSetRowAt(rowIndex);
            String columnName = rowData.get(0);
            uniqueConstraintColumns.add(columnName);
        }

        return uniqueConstraintColumns;
    }

    @Override
    short getOrdinalPositionInTheUniqueConstraint(String catalog, String table, String uniqueConstraint, String column) {
        String query = "SELECT ORDINAL_POSITION " +
                "FROM KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_NAME" + " = " + "'" + uniqueConstraint + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);
        String ordinalPosition = rowData.get(0);

        return Short.valueOf(ordinalPosition);
    }

    @Override
    Set<String> getReferentialConstraints(String catalog, String table) {
        Set<String> referentialConstraints = new CopyOnWriteArraySet<>();

        String query = "SELECT CONSTRAINT_NAME " +
                "FROM REFERENTIAL_CONSTRAINTS " +
                "WHERE CONSTRAINT_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        int rowCount = resultSet.getResultSetRowCount();
        for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
            List<String> rowData = resultSet.getResultSetRowAt(rowIndex);
            String constraintName = rowData.get(0);
            referentialConstraints.add(constraintName);
        }

        return referentialConstraints;
    }

    @Override
    Set<String> getReferencingColumns(String catalog, String table, String refConstraint) {
        Set<String> referencingColumns = new CopyOnWriteArraySet<>();

        String query = "SELECT COLUMN_NAME " +
                "FROM KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_NAME" + " = " + "'" + refConstraint + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        int rowCount = resultSet.getResultSetRowCount();
        for (int rowIndex = 1; rowIndex <= rowCount; rowIndex++) {
            List<String> rowData = resultSet.getResultSetRowAt(rowIndex);
            String columnName = rowData.get(0);
            referencingColumns.add(columnName);
        }

        return referencingColumns;
    }

    @Override
    short getOrdinalPositionInTheRefConstraint(String catalog, String table, String refConstraint, String column) {
        String query = "SELECT ORDINAL_POSITION " +
                "FROM KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_NAME" + " = " + "'" + refConstraint + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);
        String ordinalPosition = rowData.get(0);

        return Short.valueOf(ordinalPosition);
    }

    @Override
    String getReferencedTable(String catalog, String table, String refConstraint) {
        String query = "SELECT REFERENCED_TABLE_NAME " +
                "FROM REFERENTIAL_CONSTRAINTS " +
                "WHERE CONSTRAINT_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_NAME" + " = " + "'" + refConstraint + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        return rowData.get(0);
    }

    @Override
    String getColumnReferencedBy(String catalog, String table, String refConstraint, String column) {
        String query = "SELECT REFERENCED_COLUMN_NAME " +
                "FROM KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "CONSTRAINT_NAME" + " = " + "'" + refConstraint + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);
        return rowData.get(0);
    }

    @Override
    String getColumnType(String catalog, String table, String column) {
        String query = "SELECT COLUMN_TYPE " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        return rowData.get(0);
    }

    @Override
    String getSQLDataType(String catalog, String table, String column) {
        String query = "SELECT DATA_TYPE " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        return rowData.get(0);
    }

    @Override
    Optional<Integer> getNumericPrecision(String catalog, String table, String column) {
        Integer numericPrecision = null;

        String query = "SELECT NUMERIC_PRECISION " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        try {
            numericPrecision = Integer.valueOf(rowData.get(0));
        } catch (NumberFormatException e) {}

        return Optional.ofNullable(numericPrecision);
    }

    @Override
    Optional<Integer> getNumericScale(String catalog, String table, String column) {
        Integer numericScale = null;

        String query = "SELECT NUMERIC_SCALE " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        try {
            numericScale = Integer.valueOf(rowData.get(0));
        } catch (NumberFormatException e) {}

        return Optional.ofNullable(numericScale);
    }

    @Override
    Optional<Set<String>> getValueSet(String catalog, String table, String column) {
        String columnType = getColumnType(catalog, table, column);

        if (columnType.toUpperCase().startsWith("ENUM") ||
                columnType.toUpperCase().startsWith("SET")) {

            Set<String> valueSet = new CopyOnWriteArraySet<>();

            String set = columnType.substring(columnType.indexOf('(')+1, columnType.lastIndexOf(')'));
            StringTokenizer st = new StringTokenizer(set, ",");

            while (st.hasMoreTokens())
                valueSet.add(st.nextToken().trim());

            return Optional.of(valueSet);
        } else
            return Optional.empty();
    }

    @Override
    Optional<Integer> getCharacterMaximumLength(String catalog, String table, String column) {
        Integer characterMaximumLength = null;

        String query = "SELECT CHARACTER_MAXIMUM_LENGTH " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        try {
            characterMaximumLength = Integer.valueOf(rowData.get(0));
        } catch (NumberFormatException e) {}

        return Optional.ofNullable(characterMaximumLength);
    }

    @Override
    Optional<Integer> getCharacterOctetLength(String catalog, String table, String column) {
        Integer characterOctetLength = null;

        String query = "SELECT CHARACTER_OCTET_LENGTH " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        try {
            characterOctetLength = Integer.valueOf(rowData.get(0));
        } catch (NumberFormatException e) {}

        return Optional.ofNullable(characterOctetLength);
    }

    @Override
    public Optional<String> getMaximumIntegerValue(String catalog, String table, String column) {
        String maximumIntegerValue = null;

        String SQLDataType = getSQLDataType(catalog, table, column).toUpperCase();
        if (isUnsigned(catalog, table, column)) {
            switch (SQLDataType) {
                case "TINYINT": maximumIntegerValue = IntegerTypes.TINYINT_UNSIGNED_MAXIMUM_VALUE.toString(); break;
                case "SMALLINT": maximumIntegerValue = IntegerTypes.SMALLINT_UNSIGNED_MAXIMUM_VALUE.toString(); break;
                case "MEDIUMINT": maximumIntegerValue = IntegerTypes.MEDIUMINT_UNSIGNED_MAXIMUM_VALUE.toString(); break;
                case "INT": maximumIntegerValue = IntegerTypes.INT_UNSIGNED_MAXIMUM_VALUE.toString(); break;
                case "BIGINT": maximumIntegerValue = IntegerTypes.BIGINT_UNSIGNED_MAXIMUM_VALUE.toString(); break;
            }
        } else {
            switch (SQLDataType) {
                case "TINYINT": maximumIntegerValue = IntegerTypes.TINYINT_SIGNED_MAXIMUM_VALUE.toString(); break;
                case "SMALLINT": maximumIntegerValue = IntegerTypes.SMALLINT_SIGNED_MAXIMUM_VALUE.toString(); break;
                case "MEDIUMINT": maximumIntegerValue = IntegerTypes.MEDIUMINT_SIGNED_MAXIMUM_VALUE.toString(); break;
                case "INT": maximumIntegerValue = IntegerTypes.INT_SIGNED_MAXIMUM_VALUE.toString(); break;
                case "BIGINT": maximumIntegerValue = IntegerTypes.BIGINT_SIGNED_MAXIMUM_VALUE.toString(); break;
            }
        }

        return Optional.ofNullable(maximumIntegerValue);
    }

    @Override
    public Optional<String> getMinimumIntegerValue(String catalog, String table, String column) {
        String minimumIntegerValue = null;

        String SQLDataType = getSQLDataType(catalog, table, column).toUpperCase();
        if (isUnsigned(catalog, table, column)) {
            switch (SQLDataType) {
                case "TINYINT": minimumIntegerValue = IntegerTypes.TINYINT_UNSIGNED_MINIMUM_VALUE.toString(); break;
                case "SMALLINT": minimumIntegerValue = IntegerTypes.SMALLINT_UNSIGNED_MINIMUM_VALUE.toString(); break;
                case "MEDIUMINT": minimumIntegerValue = IntegerTypes.MEDIUMINT_UNSIGNED_MINIMUM_VALUE.toString(); break;
                case "INT": minimumIntegerValue = IntegerTypes.INT_UNSIGNED_MINIMUM_VALUE.toString(); break;
                case "BIGINT": minimumIntegerValue = IntegerTypes.BIGINT_UNSIGNED_MINIMUM_VALUE.toString(); break;
            }
        } else {
            switch (SQLDataType) {
                case "TINYINT": minimumIntegerValue = IntegerTypes.TINYINT_SIGNED_MINIMUM_VALUE.toString(); break;
                case "SMALLINT": minimumIntegerValue = IntegerTypes.SMALLINT_SIGNED_MINIMUM_VALUE.toString(); break;
                case "MEDIUMINT": minimumIntegerValue = IntegerTypes.MEDIUMINT_SIGNED_MINIMUM_VALUE.toString(); break;
                case "INT": minimumIntegerValue = IntegerTypes.INT_SIGNED_MINIMUM_VALUE.toString(); break;
                case "BIGINT": minimumIntegerValue = IntegerTypes.BIGINT_SIGNED_MINIMUM_VALUE.toString(); break;
            }
        }

        return Optional.ofNullable(minimumIntegerValue);
    }

    @Override
    Optional<String> getMaximumDateTimeValue(String catalog, String table, String column) {
        String maximumDateTimeValue = null;

        String SQLDataType = getSQLDataType(catalog, table, column).toUpperCase();
        switch (SQLDataType) {
            case "TIMESTAMP": break;
            case "DATETIME": maximumDateTimeValue = DateTimeTypes.DATETIME_MAXIMUM_VALUE.toString(); break;
        }

        return Optional.ofNullable(maximumDateTimeValue);
    }

    @Override
    Optional<String> getMinimumDateTimeValue(String catalog, String table, String column) {
        String minimumDateTimeValue = null;

        String SQLDataType = getSQLDataType(catalog, table, column).toUpperCase();
        switch (SQLDataType) {
            case "TIMESTAMP": break;
            case "DATETIME": minimumDateTimeValue = DateTimeTypes.DATETIME_MINIMUM_VALUE.toString(); break;
        }

        return Optional.ofNullable(minimumDateTimeValue);
    }

    @Override
    Optional<String> getMaximumDateValue() {
        return Optional.ofNullable(DateTypes.DATE_MAXIMUM_VALUE.toString());
    }

    @Override
    Optional<String> getMinimumDateValue() {
        return Optional.ofNullable(DateTypes.DATE_MINIMUM_VALUE.toString());
    }

    @Override
    boolean isUnsigned(String catalog, String table, String column) {
        boolean isUnsigned = false;

        String columnType = getColumnType(catalog, table, column);

        int firstSpace = columnType.indexOf(" ");
        int lastCloseParenthesis = columnType.lastIndexOf(")");
        int beginIndex = Integer.max(firstSpace, lastCloseParenthesis) + 1;

        if (beginIndex > -1 && beginIndex < columnType.length()) {
            String substring = columnType.substring(beginIndex);
            if (substring.toUpperCase().contains("UNSIGNED"))
                isUnsigned = true;
        }

        return isUnsigned;
    }

    @Override
    String getDefaultValue(String catalog, String table, String column) {
        String query = "SELECT COLUMN_DEFAULT " +
                "FROM COLUMNS " +
                "WHERE TABLE_SCHEMA" + " = " + "'" + catalog + "'" +
                " AND " + "TABLE_NAME" + " = " + "'" + table + "'" +
                " AND " + "COLUMN_NAME" + " = " + "'" + column + "'";

        SQLResultSet resultSet = executeQueryFromInformationSchema(query);

        List<String> rowData = resultSet.getResultSetRowAt(1);

        return rowData.get(0);
    }

    @Override
    int getJDBCDataType(String catalog, String table, String column) {
        int JDBCDataType = super.getJDBCDataType(catalog, table, column);

        if (JDBCDataType == Types.TINYINT && getNumericPrecision(catalog, table, column).get() == 1)
            JDBCDataType = Types.BOOLEAN;

        return JDBCDataType;
    }

    @Override // about SQL query
    public SQLResultSet executeQuery(String query) {
        SQLResultSet SQLRS = null;

        try {
            Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY,
                    ResultSet.HOLD_CURSORS_OVER_COMMIT);

            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();

            List<Integer> timeZoneAwareTemporalDataTypeColumnIndexes = getTimeZoneAwareTemporalDataTypeColumnIndexes(rsmd);

            SQLRS = new MySQLResultSet(rs, rsmd, getDBZoneOffset(), timeZoneAwareTemporalDataTypeColumnIndexes);

        } catch(SQLException e) { e.printStackTrace(); }

        return SQLRS;
    }

    private List<Integer> getTimeZoneAwareTemporalDataTypeColumnIndexes(ResultSetMetaData rsmd) throws SQLException {
        List<Integer> indexes = new ArrayList<>();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if (SqlXsdMap.getMappedXSD(rsmd.getColumnType(i)).equals(XSDs.XSD_DATE_TIME) && isTimeZoneAwareDataType(rsmd.getColumnTypeName(i)))
                indexes.add(i);
        }

        return indexes;
    }
}