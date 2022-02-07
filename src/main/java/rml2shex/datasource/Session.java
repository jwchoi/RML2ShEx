package rml2shex.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Paths;

class Session {
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

    Dataset<Row> load(DataSource.DataSourceKinds dataSourceKind, String path) {
        switch (dataSourceKind) {
            case CSV:
                return sparkSession.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("enforceSchema", "false")
                        .csv(path);
        }

        return null;
    }

    Dataset<Row> sql(String sqlText) { return sparkSession.sql(sqlText); }
}
