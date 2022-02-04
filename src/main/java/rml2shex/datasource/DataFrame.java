package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataFrame {
    private Dataset<Row> df;

    private List<Column> subjectColumns;
    private List<List<Column>> objectColumnsList;
    private List<DataFrame> parents;

    DataFrame(Dataset<Row> df) {
        this.df = df;

        subjectColumns = new ArrayList<>();
        objectColumnsList = new ArrayList<>();
        parents = new ArrayList<>();
    }

    void show() { df.show(); }
    void printSchema() { df.printSchema(); }

    List<Column> getSubjectColumns() { return subjectColumns; }
    void setSubjectColumns(List<Column> subjectColumns) { this.subjectColumns = subjectColumns; }

    public List<List<Column>> getObjectColumnsList() { return objectColumnsList; }
    public void addObjectColumns(List<Column> objectColumns) { objectColumnsList.add(objectColumns); }

    public List<DataFrame> getParents() { return parents; }
    void addParent(DataFrame parent) { parents.add(parent); }

    private void assignIncludeNull(Column column) {
        long nullCount = df.where(df.col(column.getName()).isNull()).count();
        column.setIncludeNull(nullCount > 0 ? true : false);
    }

    private void assignType(Column column) {
        column.setType(df.select(column.getName()).schema().apply(column.getName()).dataType().typeName());
    }

    void assignMetadata(Column column) {
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

        assignType(column);
        assignIncludeNull(column);
    }

    void countOccurrence(List<Column> subjectColumns, List<Column> objectColumns) {
        // preprocess
        List<String> sbjCols = subjectColumns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> objCols = objectColumns.stream().map(Column::getName).collect(Collectors.toList());
        String firstSbjCol = sbjCols.remove(0);

        List<String> restColList = new ArrayList<>();
        restColList.addAll(sbjCols);
        restColList.addAll(objCols);
        String[] restCols = restColList.toArray(new String[0]);

        String[] restSbjCols = sbjCols.toArray(new String[0]);

        // groupBy
        df.select(firstSbjCol, restCols).show();
        df.select(firstSbjCol, restCols).groupBy(firstSbjCol, restSbjCols).count().show();
    }
}
