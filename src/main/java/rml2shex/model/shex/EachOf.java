package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EachOf extends DeclarableTripleExpr {

    static class IdGenerator {
        private static int incrementer = 0;
        private static int getPostfix() { return incrementer++; }

        static IRI generateId(String prefixLabel, URI prefixIRI, String localPartPrefix) {
            return new IRI(prefixLabel, prefixIRI, localPartPrefix + getPostfix());
        }
    }

    private Set<TripleExpr> expressions;

    EachOf(IRI id, TripleExpr tripleExpr1, TripleExpr tripleExpr2) {
        super(Kinds.EachOf, id);

        expressions = new HashSet<>();
        expressions.add(tripleExpr1);
        expressions.add(tripleExpr2);
    }

    void addTripleExpr(TripleExpr tripleExpr) { expressions.add(tripleExpr); }

    @Override
    String getSerializedTripleExpr() {
        String serializedTripleExpr = super.getSerializedTripleExpr();
        if (serializedTripleExpr != null) return serializedTripleExpr;

        if (expressions.size() == 0) return Symbols.EMPTY;

        StringBuffer sb = new StringBuffer();

        List<TripleExpr> expressions = this.expressions.stream().collect(Collectors.toList());
        TripleExpr theFirstTripleExpr = expressions.remove(0);
        List<TripleExpr> theRestTripleExprs = expressions;

        sb.append(theFirstTripleExpr.getSerializedTripleExpr());
        theRestTripleExprs.stream().forEach(tripleExpr -> sb.append(Symbols.SEMICOLON + Symbols.NEWLINE + tripleExpr.getSerializedTripleExpr()));

        serializedTripleExpr = sb.toString();
        setSerializedTripleExpr(serializedTripleExpr);
        return serializedTripleExpr;
    }
}
