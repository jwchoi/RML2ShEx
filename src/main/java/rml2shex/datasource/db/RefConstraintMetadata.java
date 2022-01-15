package rml2shex.datasource.db;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class RefConstraintMetadata {
	private String referencingTableName;
	private String constraintName;
	private Map<Short, String> ordinalPositionAndReferencingColumnMap;
	private String referencedTableName;
	private Map<String, String> referencingColumnAndReferencedColumnMap;
	
	RefConstraintMetadata(String definedTableName, String constraintName) {
		referencingTableName = definedTableName;
		this.constraintName = constraintName;
		
		ordinalPositionAndReferencingColumnMap = new Hashtable<>();
		referencingColumnAndReferencedColumnMap = new Hashtable<>();
	}

    List<String> getReferencingColumnsByOrdinalPosition() {
	    List<String> list = new Vector<>();

	    for (short i = 1; i <= getColumnCount(); i++)
	        list.add(ordinalPositionAndReferencingColumnMap.get(i));

	    return list;
    }

	public String getReferencingTableName() {
		return referencingTableName;
	}

	void setReferencingTableName(String referencingTableName) {
		this.referencingTableName = referencingTableName;
	}

	public String getConstraintName() {
		return constraintName;
	}

	public String getReferencedTableName() {
		return referencedTableName;
	}

	void setReferencedTableName(String referencedTableName) {
		this.referencedTableName = referencedTableName;
	}
	
	void addReferencingColumnWithOrdinalPosition(short ordinalPosition, String referencingColumnName) {
		ordinalPositionAndReferencingColumnMap.put(ordinalPosition, referencingColumnName);
	}
	
	public int getColumnCount() { 
		return ordinalPositionAndReferencingColumnMap.size();
	}
	
	public String getReferencingColumnNameAt(short ordinalPosition) {
		return ordinalPositionAndReferencingColumnMap.get(ordinalPosition);
	}
	
	void addReferencingColumnAndReferencedColumnPair(String referencingColumnName, String referencedColumnName) {
		referencingColumnAndReferencedColumnMap.put(referencingColumnName, referencedColumnName);
	}
	
	public String getReferencedColumnNameBy(String referencingColumnName) {
		return referencingColumnAndReferencedColumnMap.get(referencingColumnName);
	}
}
