package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rml2shex.model.rml.JoinCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
        subjectColumns.stream().filter(Column::isIncludeNull).map(Column::getName).forEach(includeNullCols::add);
        objectColumns.stream().filter(Column::isIncludeNull).map(Column::getName).forEach(includeNullCols::add);

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
        df = df.groupBy(firstSbjCol, restSbjCols).count();
        List<Row> rows = df.select(df.col("count")).summary("max").collectAsList();
        long maxOccurs = Long.parseLong(rows.stream().map(row -> row.getString(1)).findAny().get());

        return maxOccurs;
    }

    long acquireMaxOccurs(DataSource parentDS, List<JoinCondition> joinConditions, Session session, boolean inverse) {
        // join
        df.createOrReplaceTempView("child");
        parentDS.df.createOrReplaceTempView("parent");

        StringBuffer sql = new StringBuffer("SELECT * FROM child, parent");

        if (joinConditions.size() > 0) {
            String whereClause = joinConditions.stream()
                    .map(joinCondition -> "child." + joinCondition.getChild().getName() + "=parent." + joinCondition.getParent().getName())
                    .collect(Collectors.joining(" AND ", " WHERE ", ""));
            sql.append(whereClause);
        }

        Dataset<Row> joinResultDF = session.sql(sql.toString());

        // preprocess for groupBy
        List<String> includeNullCols = new ArrayList<>();
        subjectColumns.stream().filter(col -> col.isIncludeNull()).map(col -> "child." + col.getName()).forEach(includeNullCols::add);
        parentDS.subjectColumns.stream().filter(col -> col.isIncludeNull()).map(col -> "parent." + col.getName()).forEach(includeNullCols::add);

        for (String includeNullCol: includeNullCols) { joinResultDF = joinResultDF.where(joinResultDF.col(includeNullCol).isNotNull()); }

        List<String> sbjCols;
        if (inverse) sbjCols = parentDS.subjectColumns.stream().map(col -> "parent." + col.getName()).collect(Collectors.toList());
        else sbjCols = subjectColumns.stream().map(col -> "child." + col.getName()).collect(Collectors.toList());
        String firstSbjCol = sbjCols.remove(0);
        String[] restSbjCols = sbjCols.toArray(new String[0]);

        // groupBy
        joinResultDF = joinResultDF.groupBy(firstSbjCol, restSbjCols).count();
        List<Row> rows = joinResultDF.select(joinResultDF.col("count")).summary("max").collectAsList();
        long maxOccurs = Long.parseLong(rows.stream().map(row -> row.getString(1)).findAny().get());

        return maxOccurs;
    }

    long acquireMinOccurs(DataSource parentDS, List<JoinCondition> joinConditions, Session session, boolean inverse) {
        // join
        Optional<org.apache.spark.sql.Column> joinExprs = joinConditions.stream()
                .map(joinCondition -> df.col(joinCondition.getChild().getName()).equalTo(parentDS.df.col(joinCondition.getParent().getName())))
                .reduce(org.apache.spark.sql.Column::and);

        Optional<org.apache.spark.sql.Column> childSbjsNonNull = subjectColumns.stream()
                .filter(Column::isIncludeNull)
                .map(Column::getName)
                .map(df::col)
                .map(org.apache.spark.sql.Column::isNotNull)
                .reduce(org.apache.spark.sql.Column::and);


        Dataset<Row> joinResultDF;

        if (joinExprs.isPresent()) {
            // left_anti join includes in case of "null" of join columns.
            joinResultDF = df.join(parentDS.df, joinExprs.get(), "left_anti");
            if (childSbjsNonNull.isPresent()) joinResultDF = joinResultDF.where(childSbjsNonNull.get());
        } else {
            joinResultDF = df;

            if (childSbjsNonNull.isPresent()) joinResultDF = joinResultDF.where(childSbjsNonNull.get());

            Optional<org.apache.spark.sql.Column> parentSbjsNull = parentDS.subjectColumns.stream()
                    .map(Column::getName)
                    .map(df::col)
                    .map(org.apache.spark.sql.Column::isNull)
                    .reduce(org.apache.spark.sql.Column::or);

            if (parentSbjsNull.isPresent()) joinResultDF = joinResultDF.where(parentSbjsNull.get());
        }

        // preprocess for groupBy
        List<String> sbjCols = subjectColumns.stream().map(Column::getName).collect(Collectors.toList());
        String firstSbjCol = sbjCols.remove(0);
        String[] restSbjCols = sbjCols.toArray(new String[0]);

        // groupBy
        Dataset<Row> groupByDF = df.groupBy(firstSbjCol, restSbjCols).count();


        return joinResultDF.count() > 0 ? 0 : 1;
    }
}
