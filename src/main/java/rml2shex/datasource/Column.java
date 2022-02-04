package rml2shex.datasource;

public class Column {
    private String name;
    private String type;
    private boolean includeNull;
    private String min;
    private String max;
    private boolean distinct;

    public Column(String name) { this.name = name; }

    public String getName() { return name; }

    public boolean isIncludeNull() { return includeNull; }
    void setIncludeNull(boolean includeNull) { this.includeNull = includeNull; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getMin() { return min; }
    public void setMin(String min) { this.min = min; }

    public String getMax() { return max; }
    public void setMax(String max) { this.max = max; }

    public boolean isDistinct() { return distinct; }
    public void setDistinct(boolean distinct) { this.distinct = distinct; }
}
