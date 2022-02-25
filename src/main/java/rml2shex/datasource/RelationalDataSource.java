package rml2shex.datasource;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class RelationalDataSource extends DataSource {
    private Metadata metadata;

    RelationalDataSource(DataSourceKinds kind, Session session, Dataset<Row> df, Metadata metadata) throws Exception {
        super(kind, session, df);
        this.metadata = metadata;
    }

    @Override
    void setSubjectColumns(List<Column> subjectColumns) {
        List<Column> pseudoColumns = subjectColumns.stream().map(this::getPseudoColumn).collect(Collectors.toList());
        super.setSubjectColumns(pseudoColumns);
    }

    @Override
    void acquireMinAndMaxLength(Column column) {
        Column pseudoColumn = getPseudoColumn(column);

        // for CHAR
        Optional<Column> charColumn = metadata.columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(pseudoColumn.getName()))
                .filter(colDesc -> colDesc.getType().isPresent())
                .filter(colDesc -> colDesc.getMaxLength().isPresent())
                .filter(colDesc -> Integer.parseInt(colDesc.getType().get()) == Types.CHAR)
                .findAny();

        if (charColumn.isPresent()) {
            pseudoColumn.setMinLength(String.valueOf(charColumn.get().getMaxLength().get()));
            pseudoColumn.setMaxLength(String.valueOf(charColumn.get().getMaxLength().get()));

            apply(pseudoColumn, column);
            return;
        }

        super.acquireMinAndMaxLength(pseudoColumn);

        // for BINARY, VARBINARY, LONGVARBINARY
        // for CHAR
        Optional<Column> hexBinaryColumn = metadata.columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(pseudoColumn.getName()))
                .filter(colDesc -> colDesc.getType().isPresent())
                .filter(colDesc -> Integer.parseInt(colDesc.getType().get()) == Types.BINARY
                        || Integer.parseInt(colDesc.getType().get()) == Types.VARBINARY
                        || Integer.parseInt(colDesc.getType().get()) == Types.LONGVARBINARY)
                .findAny();

        if (hexBinaryColumn.isPresent()) {
            switch (metadata.dbms) {
                case MySQL: {
                    pseudoColumn.setMinLength(String.valueOf(pseudoColumn.getMinLength().get().intValue()*2));
                    pseudoColumn.setMaxLength(String.valueOf(pseudoColumn.getMaxLength().get().intValue()*2));
                    break;
                }
                case PostgreSQL: {
                    pseudoColumn.setMinLength(String.valueOf(pseudoColumn.getMinLength().get().intValue()-2));
                    pseudoColumn.setMaxLength(String.valueOf(pseudoColumn.getMaxLength().get().intValue()-2));
                    break;
                }
                case SQLServer: break;
            }
        }

        apply(pseudoColumn, column);
    }

    @Override
    void acquireMetadata(Column column) {
        Column pseudoColumn = getPseudoColumn(column);

        super.acquireMetadata(pseudoColumn);

        apply(pseudoColumn, column);
    }

    @Override
    long acquireMinOccurs(List<Column> objectColumns) {
        List<Column> pseudoColumns = objectColumns.stream().map(this::getPseudoColumn).collect(Collectors.toList());

        return super.acquireMinOccurs(pseudoColumns);
    }

    @Override
    long acquireMaxOccurs(List<Column> objectColumns) {
        List<Column> pseudoColumns = objectColumns.stream().map(this::getPseudoColumn).collect(Collectors.toList());

        return super.acquireMaxOccurs(pseudoColumns);
    }

    @Override
    boolean isExistent(Column column) {
        return metadata.columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(column.getName()))
                .count() > 0;
    }

    private boolean isExistentInLowercase(Column column) {
        return metadata.columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(column.getName().toLowerCase()))
                .count() == 1;
    }

    private void copyExceptName(Column src, Column dest) {
        Optional<DataSourceKinds> dataSourceKind = src.getDataSourceKind();
        dest.setDataSourceKind(dataSourceKind.isPresent() ? dataSourceKind.get() : null);

        Optional<String> type = src.getType();
        dest.setType(type.isPresent() ? type.get() : null);

        Optional<String> minValue = src.getMinValue();
        dest.setMinValue(minValue.isPresent() ? minValue.get() : null);

        Optional<String> maxValue = src.getMaxValue();
        dest.setMaxValue(maxValue.isPresent() ? maxValue.get() : null);

        Optional<Integer> minLength = src.getMinLength();
        dest.setMinLength(minLength.isPresent() ? Integer.toString(minLength.get()) : null);

        Optional<Integer> maxLength = src.getMaxLength();
        dest.setMaxLength(maxLength.isPresent() ? Integer.toString(maxLength.get()) : null);
    }

    private Column getPseudoColumn(Column column) {
        // for upper/lower case problems in PostgreSQL
        if (metadata.dbms.equals(Metadata.DBMS.PostgreSQL)) {
            if (!isExistent(column) && isExistentInLowercase(column)) {
                Column fakeColumn = new Column(column.getName().toLowerCase());
                copyExceptName(column, fakeColumn);
                return fakeColumn;
            }
        }

        return column;
    }

    private void apply(Column pseudoColumn, Column originalColumn) {
        if (pseudoColumn == originalColumn) return;

        copyExceptName(pseudoColumn, originalColumn);
    }

    static class Metadata {
        private enum DBMS { MySQL, PostgreSQL, SQLServer }

        private DBMS dbms;

        private String query;

        private List<Column> columnDescriptions;

        Metadata(Database database, String tableName, String query) throws Exception {
            initQuery(tableName, query);
            collectDataSourceMetadata(database);
        }

        String getQuery() { return query; }
        private void setQuery(String query) { this.query = query; }

        private void initQuery(String tableName, String query) {
            if (query != null) { this.query = query.endsWith(";") ? query.substring(0, query.length()-1) : query; }
            else if (tableName != null) { this.query = "SELECT * FROM " + tableName; }
        }

        private void collectDataSourceMetadata(Database database) throws Exception {
            Connection connection = getConnection(database);

            setDBMSType(connection);

            collectColumnDescriptions(connection);

            rewriteQueryIfNecessary(connection);

            connection.close();
        }

        private Connection getConnection(Database database) throws Exception {
            Class.forName(database.getJdbcDriver());
            return DriverManager.getConnection(database.getJdbcDSN(), database.getUsername(), database.getPassword());
        }

        private void setDBMSType(Connection connection) throws Exception {
            switch (connection.getMetaData().getDatabaseProductName()) {
                case "MySQL": dbms = DBMS.MySQL; break;
                case "PostgreSQL": dbms = DBMS.PostgreSQL; break;
                case "Microsoft SQL Server": dbms = DBMS.SQLServer; break;
            }
        }

        private void collectColumnDescriptions(Connection connection) throws Exception {
            columnDescriptions = new ArrayList<>();

            ResultSetMetaData metaData = connection.createStatement().executeQuery(query).getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String columnType = Integer.toString(metaData.getColumnType(i)); // SQL type from java.sql.Types
                String columnDisplaySize = Integer.toString(metaData.getColumnDisplaySize(i));

                Column column = new Column(columnLabel);
                column.setType(columnType);
                column.setMaxLength(columnDisplaySize);

                columnDescriptions.add(column);
            }
        }

        private void rewriteQueryIfNecessary(Connection connection) throws Exception {
            if (!dbms.equals(DBMS.SQLServer)) return;

            if (columnDescriptions.stream().map(Column::getName).filter(String::isEmpty).count() == 0) return;

            Select stmt = (Select) CCJSqlParserUtil.parse(query);

            for (SelectItem selectItem : ((PlainSelect)stmt.getSelectBody()).getSelectItems()) {
                selectItem.accept(new SelectItemVisitorAdapter() {
                    @Override
                    public void visit(SelectExpressionItem item) {
                        if (item.getAlias() != null) return;

                        if (columnDescriptions.stream().map(Column::getName).filter(item.getExpression().toString()::equals).count() > 0) return;

                        String syntheticAlias = item.getExpression().toString().replace("\"", "\"\"");
                        item.setAlias(new Alias("\"" + syntheticAlias + "\""));
                    }
                });
            }

            // if modified
            if (!query.equals(stmt.toString())) {
                setQuery(stmt.toString());
                collectColumnDescriptions(connection);
            }
        }
    }
}
