package rml2shex.model.shex;

import rml2shex.util.IRI;
import rml2shex.util.Symbols;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class EachOf extends DeclarableTripleExpr {

    private Set<TripleExpr> expressions;

    EachOf(IRI id, TripleExpr tripleExpr1, TripleExpr tripleExpr2) {
        super(Kinds.EachOf, id);

        expressions = new HashSet<>();
        expressions.add(tripleExpr1);
        expressions.add(tripleExpr2);
    }

    EachOf(TripleExpr tripleExpr1, TripleExpr tripleExpr2) { this(null, tripleExpr1, tripleExpr2); }

    void addTripleExpr(TripleExpr tripleExpr) { expressions.add(tripleExpr); }

    @Override
    String getSerializedTripleExpr() {
        String multiElementGroup = expressions.stream()
                .map(TripleExpr::getSerializedTripleExpr)
                .sorted()
                .collect(Collectors.joining(Symbols.SEMICOLON + Symbols.NEWLINE));

        if (getId() == null) return multiElementGroup;

        StringBuffer sb = new StringBuffer(super.getSerializedTripleExpr());
        sb.append(Symbols.OPEN_PARENTHESIS + Symbols.NEWLINE);
        sb.append(multiElementGroup.indent(2));
        sb.append(Symbols.CLOSE_PARENTHESIS);

        return sb.toString();
    }
}
