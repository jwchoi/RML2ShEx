package rml2shex.datasource;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import scala.collection.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

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

        if (!Paths.get(dir, fileName).toFile().exists()) {
            System.err.println("ERROR: " + path + " does not exist.");
            return null;
        }

        return sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("enforceSchema", "false")
                .csv(path);
    }

    Dataset<Row> loadJSON(String dir, String fileName, String jsonPathExpression) {
        if (!Paths.get(dir, fileName).toFile().exists()) {
            System.err.println("ERROR: " + Paths.get(dir, fileName).toAbsolutePath() + " does not exist.");
            return null;
        }

        String path = applyJsonPath(dir, fileName, jsonPathExpression);

        Dataset<Row> df = sparkSession.read()
                        .option("multiLine", "true")
                        .json(path);

        return flatten(df, ".");
    }

    private String applyJsonPath(String dir, String fileName, String jsonPathExpression) {
        String queryResultFilePath = null;
        try {
            Configuration conf = Configuration.builder().options(Option.ALWAYS_RETURN_LIST).build();
            String queryResult = JsonPath.using(conf).parse(new File(dir, fileName)).read(jsonPathExpression).toString();
            queryResultFilePath = Files.writeString(Paths.get("temp", fileName), queryResult, StandardOpenOption.CREATE).toString();
        } catch (IOException e) { e.printStackTrace(); }

        return queryResultFilePath;
    }

    Dataset<Row> loadXML(String dir, String fileName, String xPathExpression) {
        if (!Paths.get(dir, fileName).toFile().exists()) {
            System.err.println("ERROR: " + Paths.get(dir, fileName).toAbsolutePath() + " does not exist.");
            return null;
        }

        Map<String, String> pathAndRowTag = applyXPath(dir, fileName, xPathExpression);

        Dataset<Row> df = sparkSession.read()
                .format("xml")
                .option("rowTag", pathAndRowTag.getOrDefault("rowTag", "ROWS"))
                .option("attributePrefix", "@")
                .load(pathAndRowTag.get("path"));

        df = flatten(df, "/");

        // remove the suffix "/_VALUE"
        String[] colNames = df.columns();
        String suffix = "/_VALUE";
        for (int i = 0; i < colNames.length; i++) {
            if (colNames[i].endsWith(suffix)) colNames[i] = colNames[i].substring(0, colNames[i].lastIndexOf(suffix));
        }
        df = df.toDF(colNames);

        return df;
    }

    private Map<String, String> applyXPath(String dir, String fileName, String xPathExpression) {
        Map<String, String> pathAndRowTag = new HashMap<>();

        // get an XML document
        Document document = null;
        try {
            FileInputStream fis = new FileInputStream(Paths.get(dir, fileName).toAbsolutePath().toString());
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            document = db.parse(fis);
        } catch (Exception e) { e.printStackTrace(); }

        // get a query result
        NodeList queryResult = null;
        try {
            XPath xp = XPathFactory.newInstance().newXPath();
            queryResult = (NodeList) xp.compile(xPathExpression).evaluate(document, XPathConstants.NODESET);
        } catch (Exception e) { e.printStackTrace(); }

        // transform the query result into a file
        try {
            StringWriter writer = new StringWriter();

            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

            Node queryResultNode = null;
            for (int i = 0; i < queryResult.getLength(); i++) {
                queryResultNode = queryResult.item(i);
                transformer.transform(new DOMSource(queryResultNode), new StreamResult(writer));
            }
            String queryResultFilePath = Paths.get("temp", fileName).toAbsolutePath().toString();

            FileUtils.write(new File(queryResultFilePath), writer.toString(), "UTF-8");

            // make a return value
            pathAndRowTag.put("path", queryResultFilePath);
            if (queryResultNode != null) pathAndRowTag.put("rowTag", queryResultNode.getNodeName());
        } catch (Exception e) { e.printStackTrace(); }

        return pathAndRowTag;
    }

    private Dataset<Row> flatten(Dataset<Row> df, String delimiter) {
        StructType schema = df.schema();
        int fieldCount = schema.length();
        Iterator<StructField> iterator = schema.iterator();
        while (iterator.hasNext()) {
            StructField field = iterator.next();
            String name = field.name();
            String typeName = field.dataType().typeName();
            switch (typeName) {
                case "struct":
                    df = df.select("*", name + ".*");
                    String[] existingNames = df.columns();
                    String[] newNames = existingNames;
                    for (int i = fieldCount; i < existingNames.length; i++) newNames[i] = name + delimiter + existingNames[i];
                    df = df.toDF(newNames);
                    df = df.drop(name);
                    return flatten(df, delimiter);
                case "array":
                    df = df.select(df.col("*"), functions.explode_outer(df.col(name)))
                            .drop(name)
                            .withColumnRenamed("col", name);
                    return flatten(df, delimiter);
                default:
                    break;
            }
        }

        return df;
    }

    Dataset<Row> loadDatabase(Database database, String tableName, String query) {
        DataFrameReader dfReader = sparkSession.read()
                .format("jdbc")
                .option("driver", database.getJdbcDriver())
                .option("url", database.getJdbcDSN())
                .option("user", database.getUsername())
                .option("password", database.getPassword());

        if (query != null) {
            if (query.endsWith(";")) {
                query = query.substring(0, query.length()-1);
            }

            dfReader.option("query", query);
        } else if (tableName != null) {
            dfReader.option("dbtable", tableName);
        }

        return dfReader.load();
    }

    Dataset<Row> sql(String sqlText) { return sparkSession.sql(sqlText); }
}
