package rml2shex;

import rml2shex.datasource.Database;
import rml2shex.processor.Rml2ShexConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Rml2Shex {
    private static final String PROPERTIES_FILE = "rml2shex.properties";

    public static void main(String[] args) {
        Properties properties = loadPropertiesFile(PROPERTIES_FILE);

        if (properties == null) return;

        generateShExFile(properties);
    }

    private static void generateShExFile(Properties properties) {
        String rmlPathname = properties.getProperty("rml.pathname");
        String shexPathname = properties.getProperty("shex.pathname");
        String shexBasePrefix = properties.getProperty("shex.base.prefix");
        String shexBaseIRI = properties.getProperty("shex.base.iri");

        Rml2ShexConverter converter;

        boolean useDataSource = Boolean.parseBoolean(properties.getProperty("useDataSource"));
        if (useDataSource) {
            String dataSourceFileDir = properties.getProperty("dataSource.file.dir");

            String dataSourceJdbcUrl = properties.getProperty("dataSource.jdbc.url");
            String dataSourceJdbcDriver = properties.getProperty("dataSource.jdbc.driver");
            String dataSourceJdbcUser = properties.getProperty("dataSource.jdbc.user");
            String dataSourceJdbcPassword = properties.getProperty("dataSource.jdbc.password");

            if (dataSourceJdbcUrl == null || dataSourceJdbcDriver == null || dataSourceJdbcUser == null || dataSourceJdbcPassword == null) {
                if (dataSourceFileDir != null)
                    converter = new Rml2ShexConverter(dataSourceFileDir, rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI);
                else {
                    System.err.println("dataSource.file or some sub-properties of dataSource.jdbc are not specified.");
                    converter = new Rml2ShexConverter(rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI);
                }
            } else {
                Database database = new Database(dataSourceJdbcUrl, dataSourceJdbcDriver, dataSourceJdbcUser, dataSourceJdbcPassword);
                if (dataSourceFileDir != null)
                    converter = new Rml2ShexConverter(dataSourceFileDir, database, rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI);
                else
                    converter = new Rml2ShexConverter(database, rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI);
            }
        }
        else {
            converter = new Rml2ShexConverter(rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI);
        }

        try {
            File file = converter.generateShExFile();
            System.out.println("SUCCESS: The ShEx file \"" + file.getCanonicalPath() + "\" is generated.");
        } catch (IOException e) {
            System.err.println("ERROR: To Generate the ShEx file.");
        }
    }

    private static Properties loadPropertiesFile(String propertiesFile) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesFile));
            System.out.println("The properties file is loaded.");
        } catch (Exception ex) {
            System.err.println("ERROR: To Load the properties file (" + propertiesFile + ").");
            properties = null;
        }

        return properties;
    }
}