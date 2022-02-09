package rml2shex.datasource;

import com.jayway.jsonpath.JsonPath;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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

    Dataset<Row> loadCSV(String dir, String fileName) {
        String path = Paths.get(dir, fileName).toAbsolutePath().toString();

        return sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("enforceSchema", "false")
                .csv(path);
    }

    Dataset<Row> loadJSON(String dir, String fileName, String jsonPathExpression) {
        String path = applyJsonPath(dir, fileName, jsonPathExpression);

        return sparkSession.read()
                        .option("multiLine", "true")
                        .json(path);
    }

    private String applyJsonPath(String dir, String fileName, String jsonPathExpression) {
        String queryResultFilePath = null;
        try {
            String queryResult = JsonPath.parse(new File(dir, fileName)).read(jsonPathExpression).toString();
            queryResultFilePath = Files.writeString(Paths.get("temp", fileName), queryResult, StandardOpenOption.CREATE).toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return queryResultFilePath;
    }

    Dataset<Row> sql(String sqlText) { return sparkSession.sql(sqlText); }
}
