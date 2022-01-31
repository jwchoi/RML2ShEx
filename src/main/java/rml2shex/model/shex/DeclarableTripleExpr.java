package rml2shex.model.shex;

import rml2shex.commons.IRI;
import rml2shex.commons.Symbols;

import java.net.URI;
import java.util.Optional;

public abstract class DeclarableTripleExpr extends TripleExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Optional<IRI> id;

    DeclarableTripleExpr(Kinds kind, IRI id) {
        super(kind);
        this.id = Optional.ofNullable(id);
    }

    IRI getId() { return id.isPresent() ? id.get() : null; }

    void setId(IRI id) { this.id = Optional.ofNullable(id); }

    @Override
    String getSerializedTripleExpr() { return id.isPresent() ? Symbols.DOLLAR + id.get().getPrefixedName() + Symbols.SPACE : Symbols.EMPTY; }
}
