package rml2shex.model.shex;

public abstract class ValueSetValue {
    enum Kinds { objectValue, IriStem, IriStemRange, LiteralStem, LiteralStemRange, Language, LanguageStem, LanguageStemRange }

    private Kinds kind;

    ValueSetValue(Kinds kind) { this.kind = kind; }

    Kinds getKind() { return kind; }
}
