package janus.database;

public class DBRefConstraint implements Comparable<DBRefConstraint> {

    private String tableName;
    private String refConstraintName;

    public DBRefConstraint(String tableName, String refConstraintName) {
        this.tableName = tableName;
        this.refConstraintName = refConstraintName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRefConstraintName() {
        return refConstraintName;
    }

    @Override
    public int compareTo(DBRefConstraint o) {
        int betweenTables = tableName.compareTo(o.getTableName());

        if (betweenTables != 0)
            return betweenTables;

        return refConstraintName.compareTo(o.getRefConstraintName());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DBRefConstraint) {
            if (((DBRefConstraint)obj).getTableName().equals(tableName)
                    && ((DBRefConstraint)obj).getRefConstraintName().equals(refConstraintName))
                return true;
            else
                return false;
        }

        return super.equals(obj);
    }
}

