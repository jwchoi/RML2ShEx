package rml2shex;

import rml2shex.datasource.db.DBBridge;
import rml2shex.processor.Rml2ShexConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Rml2Shex {
    private static final String PROPERTIES_FILE = "rml2shex.properties";

    public static DBBridge dbBridge;

    public static void main(String[] args) {
        Properties properties = loadPropertiesFile(PROPERTIES_FILE);

        if (properties == null) return;

        generateShExFile(properties);
    }

    private static void generateShExFile(Properties properties) {
        String dataSourceDir = properties.getProperty("dataSource.dir");
        String rmlPathname = properties.getProperty("rml.pathname");
        String shexPathname = properties.getProperty("shex.pathname");
        String shexBasePrefix = properties.getProperty("shex.base.prefix");
        String shexBaseIRI = properties.getProperty("shex.base.iri");
        boolean useDataSource = Boolean.parseBoolean(properties.getProperty("useDataSource"));

        Rml2ShexConverter converter = useDataSource ?
                new Rml2ShexConverter(dataSourceDir, rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI) :
                new Rml2ShexConverter(rmlPathname, shexPathname, shexBasePrefix, shexBaseIRI);

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