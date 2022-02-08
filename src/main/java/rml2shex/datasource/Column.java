package rml2shex.datasource;

import java.util.Optional;

public class Column {
    private String name;
    private String type; // acquired from the data source
    private boolean includeNull; // acquired from the data source
    private Optional<String> min; // acquired from the data source
    private Optional<String> max; // acquired from the data source
    private boolean distinct; // acquired from the data source

    public Column(String name) {
        this.name = name;

        min = Optional.empty();
        max = Optional.empty();
    }

    public String getName() { return name; }

    public boolean isIncludeNull() { return includeNull; }
    void setIncludeNull(boolean includeNull) { this.includeNull = includeNull; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Optional<String> getMin() { return min; }
    public void setMin(String min) { if (min != null) this.min = Optional.of(min); }

    public Optional<String> getMax() { return max; }
    public void setMax(String max) { if (max != null) this.max = Optional.of(max); }

    public boolean isDistinct() { return distinct; }
    public void setDistinct(boolean distinct) { this.distinct = distinct; }
}
