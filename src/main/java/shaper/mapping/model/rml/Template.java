package shaper.mapping.model.rml;

import janus.database.SQLSelectField;

import java.util.LinkedList;
import java.util.List;

public class Template {
    private String format;
    private List<SQLSelectField> columnNames;
    private List<String> logicalReferences;

    Template(String format, List<SQLSelectField> columnNames) {
        this.format = format;
        this.columnNames = columnNames;
    }

    Template(String format) {
        this.format = format;
        this.logicalReferences = getLogicalReferencesIn(format);
    }

    public String getFormat() { return format; }

    public List<SQLSelectField> getColumnNames() { return columnNames; }

    public int getLengthExceptColumnName() {
        String format = this.format;

        for (SQLSelectField columnName: columnNames)
            format = format.replace("{" + columnName.getColumnNameOrAlias() + "}", "");

        return format.length();
    }

    private static List<String> getLogicalReferencesIn(String template) {
        // because backslashes need to be escaped by a second backslash in the Turtle syntax,
        // a double backslash is needed to escape each curly brace,
        // and to get one literal backslash in the output one needs to write four backslashes in the template.
        template = template.replace("\\\\", "\\");

        List<String> logicalReferences = new LinkedList<>();

        int length = template.length();
        int fromIndex = 0;
        while (fromIndex < length) {
            int openBrace = template.indexOf("{", fromIndex);
            if (openBrace == -1) break;
            while (openBrace > 0 && template.charAt(openBrace - 1) == '\\')
                openBrace = template.indexOf("{", openBrace + 1);

            int closeBrace = template.indexOf("}", fromIndex);
            if (closeBrace == -1) break;
            while (closeBrace > 0 && template.charAt(closeBrace - 1) == '\\')
                closeBrace = template.indexOf("}", closeBrace + 1);

            String reference = template.substring(openBrace + 1, closeBrace);
            reference = reference.replace("\\{", "{");
            reference = reference.replace("\\}", "}");

            logicalReferences.add(reference);
            fromIndex = closeBrace + 1;
        }

        return logicalReferences;
    }
}
