package rml2shex.model.rml;

import rml2shex.datasource.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Template {
    private String format;
    private List<Column> logicalReferences;

    Template(String format) {
        this.format = format;
        logicalReferences = getLogicalReferencesIn(format).stream().map(Column::new).collect(Collectors.toList());
    }

    public String getFormat() { return format; }

    public List<Column> getLogicalReferences() { return logicalReferences; }

    private List<String> getLogicalReferencesIn(String template) {
        // because backslashes need to be escaped by a second backslash in the Turtle syntax,
        // a double backslash is needed to escape each curly brace,
        // and to get one literal backslash in the output one needs to write four backslashes in the template.
        template = template.replace("\\\\", "\\");

        List<String> logicalReferences = new ArrayList<>();

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
