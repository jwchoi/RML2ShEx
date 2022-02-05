package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import rml2shex.model.rml.LogicalSource;
import rml2shex.model.rml.LogicalTable;
import rml2shex.model.rml.Source;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;

class Session {
    enum DataSourceKinds { CSV, JSON, XML, Database }

    private SparkSession sparkSession;

    static {
        String OS = System.getProperty("os.name").toLowerCase();

        if (OS.contains("win")) { System.setProperty("hadoop.home.dir", Paths.get("winutils").toAbsolutePath().toString()); }
        else { System.setProperty("hadoop.home.dir", "/"); }
    }

    static Session createSession() {
        Session session = new Session();
        session.sparkSession = SparkSession.builder().master("local").getOrCreate();
        session.sparkSession.sparkContext().setLogLevel("OFF");
        return session;
    }

    DataSource createDataFrameFrom(LogicalSource logicalSource, String dataSourceDir) {
        Dataset<Row> df = null;

        DataSourceKinds dataSourceKind = detectDataSourceKind(logicalSource);

        switch (dataSourceKind) {
            case CSV:
                String path = new File(dataSourceDir, logicalSource.getSource().getSource().toString()).getAbsolutePath();
                df = sparkSession.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("enforceSchema", "false")
                        .csv(path);
        }

        return new DataSource(df);
    }

    private DataSourceKinds detectDataSourceKind(LogicalSource logicalSource) {
        Source source = logicalSource.getSource();
        URI referenceFormulation = logicalSource.getReferenceFormulation();
        String iterator = logicalSource.getIterator();
        String query = logicalSource.getQuery();

        if ((referenceFormulation != null && referenceFormulation.equals(URI.create("http://semweb.mmlab.be/ns/ql#CSV")))
                || source.getSource().toString().toLowerCase().endsWith(".csv"))
            return DataSourceKinds.CSV;

        return null;
    }

    static DataSource createDataFrameFrom(LogicalTable logicalTable) {
        return null;
    }

    Dataset<Row> sql(String sqlText) { return sparkSession.sql(sqlText); }
}
