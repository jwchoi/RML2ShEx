package rml2shex.datasource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Column {
    private String name;
    private Optional<String> type; // acquired from the data source

    private Optional<String> minValue; // acquired from the data source
    private Optional<String> maxValue; // acquired from the data source

    private Optional<Integer> minLength; // acquired from the data source
    private Optional<Integer> maxLength; // acquired from the data source

    public Column(String name) {
        this.name = name;

        minValue = Optional.empty();
        maxValue = Optional.empty();

        minLength = Optional.empty();
        maxLength = Optional.empty();
    }

    public String getName() { return name; }

    public Optional<String> getType() { return type; }
    void setType(String type) { if (type != null) this.type = Optional.of(type); }

    public Optional<String> getMinValue() { return minValue; }
    void setMinValue(String minValue) { if (minValue != null) this.minValue = Optional.of(minValue); }

    public Optional<String> getMaxValue() { return maxValue; }
    void setMaxValue(String maxValue) { if (maxValue != null) this.maxValue = Optional.of(maxValue); }

    public Optional<Integer> getMinLength() { return minLength; }
    void setMinLength(String minLength) { this.minLength = Optional.of(Integer.parseUnsignedInt(minLength)); }

    public Optional<Integer> getMaxLength() { return maxLength; }
    void setMaxLength(String maxLength) { this.maxLength = Optional.of(Integer.parseUnsignedInt(maxLength)); }

    Optional<Boolean> isNumeric() {
        Optional<Boolean> isNumeric = Optional.empty();

        List<String> numericType = Arrays.asList("byte", "decimal", "double", "float", "integer", "long", "short");

        if (type.isPresent()) isNumeric = numericType.contains(type.get()) ? Optional.of(true) : Optional.of(false);

        return isNumeric;

    }

    Optional<Boolean> isString() {
        Optional<Boolean> isString = Optional.empty();

        List<String> stringType = Arrays.asList("char", "string", "varchar");

        if (type.isPresent()) isString = stringType.contains(type.get()) ? Optional.of(true) : Optional.of(false);

        return isString;
    }
}
