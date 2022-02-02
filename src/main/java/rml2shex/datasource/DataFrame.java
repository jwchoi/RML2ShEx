package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataFrame {
    private Dataset<Row> df;

    DataFrame(Dataset<Row> df) { this.df = df; }

    void show() { df.show(); }
    void printSchema() { df.printSchema(); }
}
