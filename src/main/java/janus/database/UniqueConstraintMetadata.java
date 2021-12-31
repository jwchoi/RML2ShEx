package janus.database;

import java.util.Hashtable;
import java.util.Map;

public class UniqueConstraintMetadata {
    private String definedTableName;
    private String constraintName;
    private Map<Short, String> ordinalPositionAndColumnMap;

    UniqueConstraintMetadata(String definedTableName, String constraintName) {
        this.definedTableName = definedTableName;
        this.constraintName = constraintName;

        ordinalPositionAndColumnMap = new Hashtable<>();
    }

    public String getDefinedTableName() {
        return definedTableName;
    }

    void setDefinedTableName(String definedTableName) {
        this.definedTableName = definedTableName;
    }

    public String getConstraintName() {
        return constraintName;
    }

    void addColumnWithOrdinalPosition(short ordinalPosition, String columnName) {
        ordinalPositionAndColumnMap.put(ordinalPosition, columnName);
    }

    public int getColumnCount() {
        return ordinalPositionAndColumnMap.size();
    }

    public String getColumnNameAt(short ordinalPosition) {
        return ordinalPositionAndColumnMap.get(ordinalPosition);
    }
}
