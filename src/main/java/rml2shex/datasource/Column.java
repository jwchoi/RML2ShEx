package rml2shex.datasource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class Column {
    private String name;
    private Optional<String> type; // acquired from the data source
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

    public Optional<String> getType() { return type; }
    public void setType(String type) { if (type != null) this.type = Optional.of(type); }

    public Optional<String> getMin() { return min; }
    public void setMin(String min) { if (min != null) this.min = Optional.of(min); }

    public Optional<String> getMax() { return max; }
    public void setMax(String max) { if (max != null) this.max = Optional.of(max); }

    public boolean isDistinct() { return distinct; }
    public void setDistinct(boolean distinct) { this.distinct = distinct; }

    public Optional<Boolean> isNumeric() {
        Optional<Boolean> isNumeric = Optional.empty();

        List<String> numericType = Arrays.asList("byte", "decimal", "double", "float", "integer", "long", "short");

        if (type.isPresent()) isNumeric = numericType.contains(type.get()) ? Optional.of(true) : Optional.of(false);

        return isNumeric;
    }
}
