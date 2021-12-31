package shaper.mapping.model.r2rml;

import janus.database.SQLSelectField;

import java.util.List;

public class Template {
    private String format;
    private List<SQLSelectField> columnNames;

    Template(String format, List<SQLSelectField> columnNames) {
        this.format = format;
        this.columnNames = columnNames;
    }

    public String getFormat() { return format; }

    public List<SQLSelectField> getColumnNames() { return columnNames; }

    public int getLengthExceptColumnName() {
        String format = this.format;

        for (SQLSelectField columnName: columnNames)
            format = format.replace("{" + columnName.getColumnNameOrAlias() + "}", "");

        return format.length();
    }
}
