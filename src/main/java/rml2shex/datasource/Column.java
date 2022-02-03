package rml2shex.datasource;

public class Column {
    private String name;
    private String type;
    private boolean includeNull;
    private String min;
    private String max;
    private boolean isDistinct;

    public Column(String name) { this.name = name; }

    public String getName() { return name; }
}
