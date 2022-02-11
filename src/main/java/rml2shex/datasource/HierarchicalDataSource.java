package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

class HierarchicalDataSource extends DataSource {
    private Dataset<Row> df;
    private String delimiter;

    HierarchicalDataSource(Session session, Dataset<Row> df, String delimiter) {
        super(session, df);
        this.df = df;
        this.delimiter = delimiter;
    }

    @Override
    void acquireMinAndMaxValue(Column column) {
        // if column is not nested
        if (isNestedColumn(column)) {
            super.acquireMinAndMaxValue(column);
            return;
        }

        // select all columns in the top level column enclosing the target column
        // for columns exploded from array
        String prefix = column.getName().substring(0, column.getName().indexOf(delimiter)+1);
        org.apache.spark.sql.Column[] selectCols = Arrays.stream(df.columns())
                .filter(col -> col.startsWith(prefix))
                .map(this::encloseWithBackticks)
                .map(df::col)
                .toArray(org.apache.spark.sql.Column[]::new);

        Dataset<Row> selectColsDF = df.select(selectCols);

        List<Row> rows = selectColsDF.summary("min", "max").select("summary", column.getNameInBackticks()).collectAsList();
        for (Row row : rows) {
            String key = row.getString(0);
            String value = row.getString(1);

            if (key.equals("min")) { column.setMinValue(value); continue; }
            if (key.equals("max")) { column.setMaxValue(value); continue; }
        }
    }

    @Override
    void acquireMinAndMaxLength(Column column) {
        // if column is not nested
        if (isNestedColumn(column)) {
            super.acquireMinAndMaxLength(column);
            return;
        }

        // select all columns in the top level column enclosing the target column
        // for columns exploded from array
        String prefix = column.getName().substring(0, column.getName().indexOf(delimiter)+1);
        String[] selectColsAsString = Arrays.stream(df.columns()).filter(col -> col.startsWith(prefix)).toArray(String[]::new);
        org.apache.spark.sql.Column[] selectCols = Arrays.stream(selectColsAsString).map(this::encloseWithBackticks).map(df::col).toArray(org.apache.spark.sql.Column[]::new);

        String newColumn = column.getName() + "_length";
        for (int i = 0; Arrays.asList(selectColsAsString).contains(newColumn); i++) newColumn += i;

        Dataset<Row> lengthAddedDF = df.select(selectCols).withColumn(newColumn, functions.length(df.col(column.getNameInBackticks())));

        List<Row> rows = lengthAddedDF.summary("min", "max").select("summary", encloseWithBackticks(newColumn)).collectAsList();
        for (Row row : rows) {
            String key = row.getString(0);
            String value = row.getString(1);

            if (key.equals("min")) { column.setMinLength(value); continue; }
            if (key.equals("max")) { column.setMaxLength(value); continue; }
        }
    }

    private boolean isNestedColumn(Column column) { return column.getName().indexOf(delimiter) == -1; }
}