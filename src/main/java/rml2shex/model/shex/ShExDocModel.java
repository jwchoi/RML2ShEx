package rml2shex.model.shex;

import java.net.URI;
import java.util.*;

public class ShExDocModel {
    private URI baseIRI;
    private String basePrefix;

    private Map<URI, String> prefixMap;

    private Set<DeclarableShapeExpr> declarableShapeExprs;

    ShExDocModel(String basePrefix, URI baseIRI) {
        this.baseIRI = baseIRI;
        this.basePrefix = basePrefix;

        prefixMap = new TreeMap<>();
        prefixMap.put(baseIRI, basePrefix); // BASE

        declarableShapeExprs = new HashSet<>();
    }

    public Map<URI, String> getPrefixMap() { return prefixMap; }

    void addPrefixDecl(String prefix, String IRIString) {
        prefixMap.put(URI.create(IRIString), prefix);
    }

    void addDeclarableShapeExpr(DeclarableShapeExpr declarableShapeExpr) { declarableShapeExprs.add(declarableShapeExpr); }

    public URI getBaseIRI() {
        return baseIRI;
    }
    public String getBasePrefix() { return basePrefix; }

    public Set<DeclarableShapeExpr> getDeclarableShapeExprs() { return declarableShapeExprs; }
}
