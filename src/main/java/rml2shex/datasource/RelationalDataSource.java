package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class RelationalDataSource extends DataSource {
    private enum Kinds { MySQL, PostgreSQL, SQLServer }

    private Kinds kind;

    private String query;

    private Set<Column> columnDescriptions;

    RelationalDataSource(DataSourceKinds kind, Session session, Dataset<Row> df, Database database, String tableName, String query) throws Exception {
        super(kind, session, df);

        if (query != null) { this.query = query.endsWith(";") ? query.substring(0, query.length()-1) : query; }
        else if (tableName != null) { this.query = "SELECT * FROM " + tableName; }

        columnDescriptions = new HashSet<>();

        collectDataSourceMetadata(database);
    }

    private void collectDataSourceMetadata(Database database) throws Exception {
        Class.forName(database.getJdbcDriver());
        Connection connection = DriverManager.getConnection(database.getJdbcDSN(), database.getUsername(), database.getPassword());

        switch (connection.getMetaData().getDatabaseProductName()) {
            case "MySQL": kind = Kinds.MySQL; break;
            case "PostgreSQL": kind = Kinds.PostgreSQL; break;
            case "Microsoft SQL Server": kind = Kinds.SQLServer; break;
        }

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

        connection.close();
    }

    @Override
    void acquireMinAndMaxLength(Column column) {
        // for CHAR
        Optional<Column> charColumn = columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(column.getName()))
                .filter(colDesc -> colDesc.getType().isPresent())
                .filter(colDesc -> colDesc.getMaxLength().isPresent())
                .filter(colDesc -> Integer.parseInt(colDesc.getType().get()) == Types.CHAR)
                .findAny();

        if (charColumn.isPresent()) {
            column.setMinLength(String.valueOf(charColumn.get().getMaxLength().get()));
            column.setMaxLength(String.valueOf(charColumn.get().getMaxLength().get()));
            return;
        }

        super.acquireMinAndMaxLength(column);

        // for BINARY, VARBINARY, LONGVARBINARY
        // for CHAR
        Optional<Column> hexBinaryColumn = columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(column.getName()))
                .filter(colDesc -> colDesc.getType().isPresent())
                .filter(colDesc -> Integer.parseInt(colDesc.getType().get()) == Types.BINARY
                        || Integer.parseInt(colDesc.getType().get()) == Types.VARBINARY
                        || Integer.parseInt(colDesc.getType().get()) == Types.LONGVARBINARY)
                .findAny();

        if (hexBinaryColumn.isPresent()) {
            switch (kind) {
                case MySQL: {
                    column.setMinLength(String.valueOf(column.getMinLength().get().intValue()*2));
                    column.setMaxLength(String.valueOf(column.getMaxLength().get().intValue()*2));
                    break;
                }
                case PostgreSQL: {
                    column.setMinLength(String.valueOf(column.getMinLength().get().intValue()-2));
                    column.setMaxLength(String.valueOf(column.getMaxLength().get().intValue()-2));
                    break;
                }
                case SQLServer: break;
            }
        }
    }
}
