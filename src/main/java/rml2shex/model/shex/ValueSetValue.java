package rml2shex.model.shex;

public abstract class ValueSetValue {
    enum Kinds { objectValue, IriStem, IriStemRange, LiteralStem, LiteralStemRange, Language, LanguageStem, LanguageStemRange }

    private Kinds kind;

    private String serializedValueSetValue;

    ValueSetValue(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }

    String getSerializedValueSetValue() { return serializedValueSetValue; }
    void setSerializedValueSetValue(String serializedValueSetValue) { this.serializedValueSetValue = serializedValueSetValue; }

    @Override
    public String toString() {
        if (serializedValueSetValue == null) serializedValueSetValue = getSerializedValueSetValue();

        return serializedValueSetValue;
    }
}
