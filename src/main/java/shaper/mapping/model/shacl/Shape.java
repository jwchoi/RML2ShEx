package shaper.mapping.model.shacl;

import shaper.mapping.Symbols;
import shaper.mapping.model.r2rml.Template;

import java.net.URI;
import java.util.Optional;

public abstract class Shape implements Comparable<Shape> {
    private URI id;
    private ShaclDocModel shaclDocModel;

    private String serializedShape;

    Shape(URI id, ShaclDocModel shaclDocModel) {
        this.id = id;
        this.shaclDocModel = shaclDocModel;
    }

    protected URI getID() { return id; }
    protected ShaclDocModel getShaclDocModel() { return shaclDocModel; }

    protected String getSerializedShape() { return serializedShape; }
    protected void setSerializedShape(String serializedShape) { this.serializedShape = serializedShape; }

    @Override
    public int compareTo(Shape o) {
        return id.compareTo(o.id);
    }

    protected boolean isPossibleToHavePattern(Optional<Template> template) {
        if (template.isPresent()) {
            if (template.get().getLengthExceptColumnName() > 0)
                return true;
        }

        return false;
    }

    // \n\t
    protected String getNT() { return Symbols.NEWLINE + Symbols.TAB; }
    // ;\n\t
    protected String getSNT() { return Symbols.SPACE + Symbols.SEMICOLON + Symbols.NEWLINE + Symbols.TAB; }
    // .\n\t
    protected String getDNT() { return Symbols.SPACE + Symbols.DOT + Symbols.NEWLINE + Symbols.TAB; }

    protected String getPO(String p, String o) { return p + Symbols.SPACE + o; }

    // Unlabeled Blank Node
    protected String getUBN(String p, String o) { return Symbols.OPEN_BRACKET + Symbols.SPACE + p + Symbols.SPACE + o + Symbols.SPACE + Symbols.CLOSE_BRACKET; }
}
