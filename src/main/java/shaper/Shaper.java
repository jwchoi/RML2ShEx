package shaper;

import janus.database.*;
import shaper.mapping.rdf.DirectMappingRDFMapper;
import shaper.mapping.rdf.R2RMLRDFMapper;
import shaper.mapping.rdf.RDFMapper;
import shaper.mapping.shacl.DirectMappingShaclMapper;
import shaper.mapping.shacl.R2RMLShaclMapper;
import shaper.mapping.shacl.ShaclMapper;
import shaper.mapping.shex.DirectMappingShExMapper;
import shaper.mapping.shex.R2RMLShExMapper;
import shaper.mapping.shex.RMLShExMapper;
import shaper.mapping.shex.ShExMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Shaper {
	private static final String SHAPER_PROPERTIES_FILE = "shaper.properties";

	public static final String DEFAULT_DIR_FOR_RDF_FILE = "./output/rdf/";
	public static final String DEFAULT_DIR_FOR_SHACL_FILE = "./output/shacl/";
	public static final String DEFAULT_DIR_FOR_SHEX_FILE = "./output/shex/";

	private static Properties properties;
	
	public static String rdfBaseURI;
	public static String prefixForRDFBaseURI;

	public static String shapeBaseURI;
	public static String prefixForShapeBaseURI;

	public static DBMSTypes DBMSType;
	public static DBBridge dbBridge;
	public static DBSchema dbSchema;

	public static RDFMapper rdfMapper;
	public static ShExMapper shexMapper;
	public static ShaclMapper shaclMapper;

	public static void main (String[] args) {

		if (!readPropertiesFile(SHAPER_PROPERTIES_FILE))
			return;

		rdfBaseURI = properties.getProperty("dm.rdf.base.iri");
		prefixForRDFBaseURI = properties.getProperty("dm.rdf.base.prefix");

		shapeBaseURI = properties.getProperty("shape.base.iri");
		prefixForShapeBaseURI = properties.getProperty("shape.base.prefix");

		if (!connectDatabase())
			return;

		if (!buildDBSchema())
			return;

		String generationType = properties.getProperty("generation.type");

		if (generationType.equals("both") || generationType.equals("data")) {
			if (!generateRDFFile())
				return;
		}

		if (generationType.equals("both") || generationType.equals("shape")) {
			if (!generateShapeFile())
				return;
		}

		System.out.println("It's all done. See you later!");
	}

	private static boolean generateShapeFile() {
		String shapeType = properties.getProperty("shape.type");

		switch (shapeType) {
			case "shex": {
				if (!createShExMapper()) return false;
				File file = shexMapper.generateShExFile();
				try {
					System.out.println("The ShEx file \"" + file.getCanonicalPath() + "\" is generated.");
				} catch (IOException e) {
					System.out.println("The ShEx file \"" + file.getAbsolutePath() + "\" is generated.");
				}
				break;
			}
			case "shacl": {
				if (!createShaclMapper()) return false;
				File file = shaclMapper.generateShaclFile();
				try {
					System.out.println("The Shacl file \"" + file.getCanonicalPath() + "\" is generated.");
				} catch (IOException e) {
					System.out.println("The Shacl file \"" + file.getAbsolutePath() + "\" is generated.");
				}
				break;
			}
		}

		return true;
	}

	private static boolean createShaclMapper() {
		ShaclMapper shaclMapper = null;

		String mappingType = properties.getProperty("mapping.type");

		switch (mappingType) {
			case "dm": shaclMapper = new DirectMappingShaclMapper(); break;
			case "r2rml": shaclMapper = new R2RMLShaclMapper(properties.getProperty("mapping.file")); break;
		}

		if (shaclMapper != null) {
			Shaper.shaclMapper = shaclMapper;
			return true;
		}

		return false;
	}

	private static boolean createShExMapper() {
		ShExMapper shexMapper = null;

		String mappingType = properties.getProperty("mapping.type");

		switch (mappingType) {
			case "dm": shexMapper = new DirectMappingShExMapper(); break;
			case "r2rml": shexMapper = new R2RMLShExMapper(properties.getProperty("mapping.file")); break;
			case "rml": shexMapper = new RMLShExMapper(properties.getProperty("mapping.file")); break;
		}

		if (shexMapper != null) {
			Shaper.shexMapper = shexMapper;
			return true;
		}

		return false;
	}

	private static boolean createRDFMapper() {
		RDFMapper rdfMapper = null;

		String mappingType = properties.getProperty("mapping.type");

		switch (mappingType) {
			case "dm": rdfMapper = new DirectMappingRDFMapper(); break;
			case "r2rml": rdfMapper = new R2RMLRDFMapper(properties.getProperty("mapping.file")); break;
		}

		if (rdfMapper != null) {
			Shaper.rdfMapper = rdfMapper;
			return true;
		}

		return false;
	}

	private static boolean generateRDFFile() {
		if (rdfMapper == null) {
			if (!createRDFMapper())
				return false;
		}

		File file = rdfMapper.generateRDFFile();

		try {
			System.out.println("The RDF file \"" + file.getCanonicalPath() + "\" is generated.");
		} catch (IOException e) {
			System.out.println("The RDF file \"" + file.getAbsolutePath() + "\" is generated.");
		}

		return true;
	}

	private static boolean buildDBSchema() {
		DBSchema dbSchema;
		dbSchema = DBSchemaFactory.generateLocalDatabaseMetaData(dbBridge);

		if (dbSchema != null) {
			Shaper.dbSchema = dbSchema;
			System.out.println("The schema connected is built in local.");
			return true;
		}

		System.err.println("Building the local database schema was failed.");
		return false;
	}

	private static boolean connectDatabase() {
		String driver = properties.getProperty("db.driver");

		if (DBMSTypes.MARIADB.driver().equals(driver))
			DBMSType = DBMSTypes.MARIADB;
		else if (DBMSTypes.MYSQL.driver().equals(driver))
			DBMSType = DBMSTypes.MYSQL;
		else if (DBMSTypes.ORACLE.driver().equals(driver))
			DBMSType = DBMSTypes.ORACLE;
		else if (DBMSTypes.POSTGRESQL.driver().equals(driver))
			DBMSType = DBMSTypes.POSTGRESQL;

		String host = properties.getProperty("db.host");
		String port = properties.getProperty("db.port");
		String id = properties.getProperty("db.id");
		String password = properties.getProperty("db.password");
		String schema = properties.getProperty("db.schema");

		DBBridge dbBridge = DBBridgeFactory.getDBBridge(DBMSType, host, port, id, password, schema);

		if(dbBridge != null) {
			Shaper.dbBridge = dbBridge;
			System.out.println("The database is connected.");
			return true;
		}

		System.err.println("Could not connect to the DBMS.");
		return false;
	}

	private static boolean readPropertiesFile(String propertiesFile) {
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(propertiesFile));
		} catch (Exception ex) {
			System.err.println("Error reading properties file (" + propertiesFile + ").");
			properties = null;
		}

		if (properties != null) {
			Shaper.properties = properties;
			System.out.println("The properties file is loaded.");
			return true;
		}

		return false;
	}
}