package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.*;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class RelationalDataSource extends DataSource {
    private enum Kinds { QUERY, TABLE }

    private Kinds kind;

    private Optional<String> table;
    private Optional<String> query;

    private Set<Column> columnDescriptions;

    RelationalDataSource(DataSourceKinds kind, Session session, Dataset<Row> df, Database database, String tableName, String query) throws Exception {
        super(kind, session, df);

        if (query != null) {
            this.kind = Kinds.QUERY;
            this.query = query.endsWith(";") ? Optional.of(query.substring(0, query.length()-1)) : Optional.of(query);
            table = Optional.empty();
        } else if (tableName != null) {
            this.kind = Kinds.TABLE;
            table = Optional.of(tableName);
            this.query = Optional.empty();
        }

        columnDescriptions = new HashSet<>();

        collectDataSourceMetadata(database);
    }

    private void collectDataSourceMetadata(Database database) throws Exception {
        Class.forName(database.getJdbcDriver());
        Connection connection = DriverManager.getConnection(database.getJdbcDSN(), database.getUsername(), database.getPassword());

        switch (kind) {
            case TABLE: {
                ResultSet columns = connection.getMetaData().getColumns(null, null, table.get(), null);
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    String datatype = columns.getString("DATA_TYPE"); // int => SQL type from java.sql.Types
                    String columnSize = columns.getString("COLUMN_SIZE");

                    Column column = new Column(columnName);
                    column.setType(datatype);
                    column.setMaxLength(columnSize);

                    columnDescriptions.add(column);
                }
                break;
            }
            case QUERY: {
                ResultSetMetaData metaData = connection.createStatement().executeQuery(query.get()).getMetaData();
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
        }

        connection.close();
    }

    @Override
    void acquireMinAndMaxLength(Column column) {
        super.acquireMinAndMaxLength(column);

        Optional<Column> matchedColumnDescription = columnDescriptions.stream()
                .filter(colDesc -> colDesc.getName().equals(column.getName()))
                .filter(colDesc -> colDesc.getType().isPresent())
                .filter(colDesc -> colDesc.getMaxLength().isPresent())
                .filter(colDesc -> Integer.parseInt(colDesc.getType().get()) == Types.CHAR)
                .findAny();

        if (matchedColumnDescription.isPresent()) {
            column.setMaxLength(String.valueOf(matchedColumnDescription.get().getMaxLength().get()));
        }
    }
}
