package rml2shex.mapping.model.shex;

import rml2shex.util.ID;
import rml2shex.mapping.model.rml.ObjectMap;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class ShExSchema {
    private URI baseIRI;
    private String basePrefix;

    private Map<URI, String> prefixMap;

    private Set<ShapeExpr> shapes;

    ShExSchema(String basePrefix, URI baseIRI) {
        this.baseIRI = baseIRI;
        this.basePrefix = basePrefix;

        prefixMap = new TreeMap<>();
        prefixMap.put(baseIRI, basePrefix); // BASE

        shapes = new HashSet<>();
    }

    public Map<URI, String> getPrefixMap() { return prefixMap; }

    void addPrefixDecl(String prefix, String IRIString) {
        prefixMap.put(URI.create(IRIString), prefix);
    }

    void addShapeExpr(ShapeExpr shapeExpr) { shapes.add(shapeExpr); }

    public URI getBaseIRI() {
        return baseIRI;
    }
    public String getBasePrefix() { return basePrefix; }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    static Set<Set<Shape>> createSetsForDerivedShapes(Set<Shape> baseShapes) {
        List<Shape> baseShapeList = new ArrayList<>(baseShapes);
        int size = baseShapeList.size();

        Set<Set<Shape>> setsForDerivedShapes = new CopyOnWriteArraySet<>();

        for (int i = 0; i < (1 << size); i++) {
            int oneBitsCount = Integer.bitCount(i);
            if (Integer.bitCount(i) > 1) {
                Set<Shape> set = new CopyOnWriteArraySet<>();
                StringBuffer binaryString = new StringBuffer(Integer.toBinaryString(i)).reverse();
                for (int j = 0, fromIndex = 0; j < oneBitsCount; j++) {
                    fromIndex = binaryString.indexOf("1", fromIndex);
                    set.add(baseShapeList.get(fromIndex++));
                }
                setsForDerivedShapes.add(set);
            }
        }

        return setsForDerivedShapes;
    }
}
