package rml2shex.datasource;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
        } catch (IOException e) { e.printStackTrace(); }

        return queryResultFilePath;
    }

    Dataset<Row> loadXML(String dir, String fileName, String xPathExpression) {
        Map<String, String> pathAndRowTag = applyXPath(dir, fileName, xPathExpression);

        return sparkSession.read()
                .format("xml")
                .option("rowTag", pathAndRowTag.get("rowTag"))
                .option("attributePrefix", "@")
                .load(pathAndRowTag.get("path"));
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
            pathAndRowTag.put("rowTag", queryResultNode.getNodeName());
        } catch (Exception e) { e.printStackTrace(); }

        return pathAndRowTag;
    }

    Dataset<Row> sql(String sqlText) { return sparkSession.sql(sqlText); }
}
