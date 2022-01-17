package rml2shex.mapping.model.shex;

import rml2shex.util.Id;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class EachOf extends TripleExpr {

    private static int incrementer = 0;

    static int getIncrementer() { return incrementer++; }

    private Optional<Id> id;
    private Set<TripleExpr> expressions;

    EachOf(Id id, TripleExpr tripleExpr1, TripleExpr tripleExpr2) {
        super(Kinds.EachOf);
        this.id = Optional.ofNullable(id);

        expressions = new HashSet<>();
        expressions.add(tripleExpr1);
        expressions.add(tripleExpr2);
    }

    Id getID() { return id.isPresent() ? id.get() : null; }

    void addTripleExpr(TripleExpr tripleExpr) { expressions.add(tripleExpr); }
}
