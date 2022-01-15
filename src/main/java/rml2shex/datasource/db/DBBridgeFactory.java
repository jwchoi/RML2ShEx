package rml2shex.datasource.db;

public class DBBridgeFactory {
	public static DBBridge getDBBridge(DBMSTypes DBMS_TYPE, String host, String port, String id, String password, String schema) {
		DBBridge dbBridge = null;
		
		if(DBMS_TYPE.equals(DBMSTypes.MYSQL))
			dbBridge = MySQLBridge.getInstance(host, port, id, password, schema);
		else if(DBMS_TYPE.equals(DBMSTypes.MARIADB))
			dbBridge = MariaDBBridge.getInstance(host, port, id, password, schema);
		
		return dbBridge;
	}
}
