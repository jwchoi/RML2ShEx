package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import rml2shex.model.rml.JoinCondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DataSource {

    enum DataSourceKinds { CSV, JSON, XML, Database }

    private Session session;
    private Dataset<Row> df;

    private List<Column> subjectColumns; // as key columns, but not guaranteed unique and non-null

    DataSource(Session session, Dataset<Row> df) {
        this.session = session;
        this.df = df;
    }

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

    long acquireMinOccurs(List<Column> objectColumns) {
        List<String> sbjCols = subjectColumns.stream().map(Column::getName).collect(Collectors.toList());

        Optional<org.apache.spark.sql.Column> sbjsNonNull = sbjCols.stream()
                .map(df::col)
                .map(org.apache.spark.sql.Column::isNotNull)
                .reduce(org.apache.spark.sql.Column::and);

        Dataset<Row> sbjsNonNullDF = sbjsNonNull.isPresent() ? df.where(sbjsNonNull.get()) : df;

        Optional<org.apache.spark.sql.Column> objsNull = objectColumns.stream()
                    .map(Column::getName)
                    .map(df::col)
                    .map(org.apache.spark.sql.Column::isNull)
                    .reduce(org.apache.spark.sql.Column::or);

        Dataset<Row> nonMappedRowsDF = objsNull.isPresent() ? sbjsNonNullDF.where(objsNull.get()) : sbjsNonNullDF;

        long countOfNonMappedRows = nonMappedRowsDF.count();

        if (countOfNonMappedRows == 0) return 1;

        // groupBy
        String firstSbjCol = sbjCols.remove(0);
        String[] restSbjCols = sbjCols.toArray(new String[0]);

        Dataset<Row> groupOfNonMappedRowsDF = nonMappedRowsDF.groupBy(firstSbjCol, restSbjCols).count();

        Dataset<Row> groupDF = df.groupBy(firstSbjCol, restSbjCols).count();

        // join
        Optional<org.apache.spark.sql.Column> joinExprs = Arrays.stream(groupOfNonMappedRowsDF.columns())
                .map(col -> groupOfNonMappedRowsDF.col(col).equalTo(groupDF.col(col)))
                .reduce(org.apache.spark.sql.Column::and);

        long countOfGroupsOfWhichMinOccursAreZero = groupOfNonMappedRowsDF.join(groupDF, joinExprs.get()).count();

        return countOfGroupsOfWhichMinOccursAreZero > 0 ? 0 : 1;
    }

    long acquireMaxOccurs(List<Column> objectColumns) {
        // preprocess
        List<String> sbjCols = subjectColumns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> objCols = objectColumns.stream().map(Column::getName).collect(Collectors.toList());

        List<String> SOCols = new ArrayList<>();
        sbjCols.stream().forEach(SOCols::add);
        objCols.stream().forEach(SOCols::add);

        Optional<org.apache.spark.sql.Column> conditionNonNull = SOCols.stream()
                .map(df::col)
                .map(org.apache.spark.sql.Column::isNotNull)
                .reduce(org.apache.spark.sql.Column::and);

        String firstSbjCol = sbjCols.remove(0);
        String[] restSbjCols = sbjCols.toArray(new String[0]);

        List<String> restColList = new ArrayList<>();
        restColList.addAll(sbjCols);
        restColList.addAll(objCols);
        String[] restCols = restColList.toArray(new String[0]);

        // groupBy
        Dataset<Row> SONonNullDF = conditionNonNull.isPresent() ? df.select(firstSbjCol, restCols).where(conditionNonNull.get()).distinct() : df.select(firstSbjCol, restCols).distinct();
        Dataset<Row> groupByDF = SONonNullDF.groupBy(firstSbjCol, restSbjCols).count();
        List<Row> rows = groupByDF.select(groupByDF.col("count")).summary("max").collectAsList();
        long maxOccurs = Long.parseLong(rows.stream().map(row -> row.getString(1)).findAny().get());

        return maxOccurs;
    }

    long acquireMinOccurs(DataSource parentDataSource, List<JoinCondition> joinConditions, boolean inverse) {
        if (joinConditions.size() == 0) {
            return inverse ? parentDataSource.acquireMinOccurs(subjectColumns) : acquireMinOccurs(parentDataSource.subjectColumns);
        }

        // join
        Optional<org.apache.spark.sql.Column> joinExprs = joinConditions.stream()
                .map(joinCondition -> df.col(joinCondition.getChild().getName()).equalTo(parentDataSource.df.col(joinCondition.getParent().getName())))
                .reduce(org.apache.spark.sql.Column::and);

        DataSource childDS = inverse ? parentDataSource : this;
        DataSource parentDS = inverse ? this : parentDataSource;

        // left_anti join includes in case of "null" of join columns.
        Dataset<Row> joinResultDF = childDS.df.join(parentDS.df, joinExprs.get(), "left_anti");

        List<String> childSbjCols = childDS.subjectColumns.stream().map(Column::getName).collect(Collectors.toList());

        Optional<org.apache.spark.sql.Column> childSbjsNonNull = childSbjCols.stream()
                .map(childDS.df::col)
                .map(org.apache.spark.sql.Column::isNotNull)
                .reduce(org.apache.spark.sql.Column::and);

        if (childSbjsNonNull.isPresent()) joinResultDF = joinResultDF.where(childSbjsNonNull.get());

        long countOfNonMatchedRows = joinResultDF.count();

        if (countOfNonMatchedRows == 0) return 1;

        // groupBy after join
        String firstSbjCol = childSbjCols.remove(0);
        String[] restSbjCols = childSbjCols.toArray(new String[0]);

        Dataset<Row> groupAfterJoinDF = joinResultDF.groupBy(firstSbjCol, restSbjCols).count();

        // groupBy
        Dataset<Row> groupByDF = childSbjsNonNull.isPresent() ? childDS.df.where(childSbjsNonNull.get()).groupBy(firstSbjCol, restSbjCols).count() : childDS.df.groupBy(firstSbjCol, restSbjCols).count();

        // join with "groupBy after join" and "groupBy with original df"
        joinExprs = Arrays.stream(groupAfterJoinDF.columns())
                .map(col -> groupAfterJoinDF.col(col).equalTo(groupByDF.col(col)))
                .reduce(org.apache.spark.sql.Column::and);

        long countOfGroupsOfWhichMinOccursAreZero = groupAfterJoinDF.join(groupByDF, joinExprs.get()).count();

        return countOfGroupsOfWhichMinOccursAreZero > 0 ? 0 : 1;
    }

    long acquireMaxOccurs(DataSource parentDS, List<JoinCondition> joinConditions, boolean inverse) {
        if (joinConditions.size() == 0) {
            return inverse ? parentDS.acquireMaxOccurs(subjectColumns) : acquireMaxOccurs(parentDS.subjectColumns);
        }

        // join
        df.createOrReplaceTempView("child");
        parentDS.df.createOrReplaceTempView("parent");

        StringBuffer sql = new StringBuffer("SELECT * FROM child, parent");

        String whereClause = joinConditions.stream()
                .map(joinCondition -> "child." + joinCondition.getChild().getName() + "=parent." + joinCondition.getParent().getName())
                .collect(Collectors.joining(" AND ", " WHERE ", ""));
        sql.append(whereClause);


        Dataset<Row> joinResultDF = session.sql(sql.toString());

        // preprocess for groupBy
        List<String> childSbjs = subjectColumns.stream().map(col -> "child." + col.getName()).collect(Collectors.toList());
        List<String> parentSbjs = parentDS.subjectColumns.stream().map(col -> "parent." + col.getName()).collect(Collectors.toList());
        List<String> bothSbjs = new ArrayList<>();
        childSbjs.stream().forEach(bothSbjs::add);
        parentSbjs.stream().forEach(bothSbjs::add);

        Optional<org.apache.spark.sql.Column> conditionNonNull = bothSbjs.stream()
                .map(joinResultDF::col)
                .map(org.apache.spark.sql.Column::isNotNull)
                .reduce(org.apache.spark.sql.Column::and);

        joinResultDF = conditionNonNull.isPresent() ? joinResultDF.where(conditionNonNull.get()) : joinResultDF;

        String firstOutOfBothSbjCol = bothSbjs.remove(0);
        String[] restOutOfBothSbjCols = bothSbjs.toArray(new String[0]);

        List<String> sbjCols = inverse ? parentSbjs : childSbjs;
        String firstSbjCol = sbjCols.remove(0);
        String[] restSbjCols = sbjCols.toArray(new String[0]);

        // groupBy
        joinResultDF = joinResultDF.select(firstOutOfBothSbjCol, restOutOfBothSbjCols).distinct().groupBy(firstSbjCol, restSbjCols).count();
        List<Row> rows = joinResultDF.select(joinResultDF.col("count")).summary("max").collectAsList();
        long maxOccurs = Long.parseLong(rows.stream().map(row -> row.getString(1)).findAny().get());

        return maxOccurs;
    }
}
