package rml2shex.mapping.model.shex;

import rml2shex.util.Id;

import java.net.URI;
import java.util.Optional;

public class ShapeOr extends ShapeExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static Id generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new Id(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Optional<Id> id;

    ShapeOr(Id id) {
        super(Kinds.ShapeOr);
        this.id = Optional.ofNullable(id);
    }
}
