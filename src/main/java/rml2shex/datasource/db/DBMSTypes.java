package rml2shex.datasource.db;

public enum DBMSTypes {
	MYSQL("org.gjt.mm.mysql.Driver"),
	MARIADB("org.mariadb.jdbc.Driver"),
	ORACLE("oracle.jdbc.driver.OracleDriver"),
	POSTGRESQL("org.postgresql.Driver");
	
	private final String driver;
	
	private DBMSTypes(String driver){
		this.driver = driver;
	}
	
	public String driver() { return driver; }
}
