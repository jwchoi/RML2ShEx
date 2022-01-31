package rml2shex.model.shex;

import rml2shex.commons.IRI;
import rml2shex.commons.Symbols;

import java.net.URI;
import java.util.Optional;

public abstract class DeclarableShapeExpr extends ShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;

        private static int getPostfix() {
            return incrementer++;
        }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Optional<IRI> id;

    DeclarableShapeExpr(Kinds kind, IRI id) {
        super(kind);
        this.id = Optional.ofNullable(id);
    }

    IRI getId() { return id.isPresent() ? id.get() : null; }

    void setId(IRI id) { this.id = Optional.ofNullable(id); }

    @Override
    public String getSerializedShapeExpr() { return id.isPresent() ? id.get().getPrefixedName() + Symbols.SPACE : Symbols.EMPTY; }

    @Override
    public int compareTo(ShapeExpr o) {
        if (!o.getKind().equals(Kinds.shapeExprRef)) {
            if (id.isPresent() && ((DeclarableShapeExpr) o).id.isPresent()) {
                return id.get().compareTo(((DeclarableShapeExpr) o).id.get());
            }
        }

        return super.compareTo(o);
    }
}
