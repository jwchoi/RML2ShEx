package rml2shex.datasource;

public class Column {
    private String name;
    private String type; // acquired from the data source
    private boolean includeNull; // acquired from the data source
    private String min; // acquired from the data source
    private String max; // acquired from the data source
    private boolean distinct; // acquired from the data source

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
