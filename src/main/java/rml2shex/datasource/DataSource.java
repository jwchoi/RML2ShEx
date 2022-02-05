package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rml2shex.model.rml.JoinCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataSource {
    private Dataset<Row> df;

    private List<Column> subjectColumns;

    DataSource(Dataset<Row> df) { this.df = df; }

    void setSubjectColumns(List<Column> subjectColumns) { this.subjectColumns = subjectColumns; }

    private void acquireIncludeNull(Column column) {
        long nullCount = df.where(df.col(column.getName()).isNull()).count();
        column.setIncludeNull(nullCount > 0 ? true : false);
    }

    private void acquireType(Column column) {
        column.setType(df.select(column.getName()).schema().apply(column.getName()).dataType().typeName());
    }

    void acquireMetadata(Column column) {
        Dataset<Row> colDF = df.select(column.getName());

        String[] attributes = {"min", "max", "count", "count_distinct"};

        long count = 0;
        long count_distinct = 0;

        List<Row> rows = colDF.summary(attributes).collectAsList();
        for (Row row : rows) {
            String key = row.getString(0);
            String value = row.getString(1);

           if (key.equals(attributes[0])) { column.setMin(value); continue; }
           if (key.equals(attributes[1])) { column.setMax(value); continue; }

           if (key.equals(attributes[2])) { count = Long.parseLong(value); continue; }
           if (key.equals(attributes[3])) { count_distinct = Long.parseLong(value); continue; }
        }

        column.setDistinct(count == count_distinct ? true : false);

        acquireType(column);
        acquireIncludeNull(column);
    }

    long acquireMaxOccurs(List<Column> objectColumns) {
        // preprocess
        List<String> includeNullCols = new ArrayList<>();
        subjectColumns.stream().filter(col -> col.isIncludeNull()).map(Column::getName).forEach(includeNullCols::add);
        objectColumns.stream().filter(col -> col.isIncludeNull()).map(Column::getName).forEach(includeNullCols::add);

        List<String> sbjCols = subjectColumns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> objCols = objectColumns.stream().map(Column::getName).collect(Collectors.toList());

        String firstSbjCol = sbjCols.remove(0);

        List<String> restColList = new ArrayList<>();
        restColList.addAll(sbjCols);
        restColList.addAll(objCols);
        String[] restCols = restColList.toArray(new String[0]);

        String[] restSbjCols = sbjCols.toArray(new String[0]);

        // groupBy
        Dataset<Row> df = this.df.select(firstSbjCol, restCols);
        for (String includeNullCol: includeNullCols) { df = df.where(df.col(includeNullCol).isNotNull()); }
        df = df.select(firstSbjCol, restCols).groupBy(firstSbjCol, restSbjCols).count();
        List<Row> rows = df.select(df.col("count")).summary("max").collectAsList();
        long maxOccurs = Long.parseLong(rows.stream().map(row -> row.getString(1)).findAny().get());

        return maxOccurs;
    }

    long acquireMaxOccurs(DataSource parentDS, List<JoinCondition> joinConditions, Session session) {
        // join
        Dataset<Row> joinResultDF = df;

        if (joinConditions.size() > 0) {
            df.createOrReplaceTempView("child");
            parentDS.df.createOrReplaceTempView("parent");

            StringBuffer sql = new StringBuffer("SELECT * FROM child, parent WHERE ");

            String whereClause = joinConditions.stream()
                    .map(joinCondition -> "child." + joinCondition.getChild().getName() + "=parent." + joinCondition.getParent().getName())
                    .collect(Collectors.joining(" AND "));

            sql.append(whereClause);

            System.out.println(sql);

            joinResultDF = session.sql(sql.toString());
        }

        joinResultDF.show();

        // preprocess for groupBy



        // groupBy


        return -1;
    }
}
