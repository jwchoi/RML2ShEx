package shaper.mapping.model.shacl;

import shaper.mapping.PrefixMap;
import shaper.mapping.Symbols;
import shaper.mapping.model.dm.TableIRI;

import java.net.URI;
import java.util.*;

public class ShaclDocModel {
    private URI baseIRI;
    private String prefix;

    private Map<URI, String> prefixMap;

    private Set<Shape> shapes;

    ShaclDocModel(URI baseIRI, String prefix) {
        this.baseIRI = baseIRI;
        this.prefix = prefix;

        prefixMap = new TreeMap<>();
        prefixMap.put(URI.create(baseIRI + Symbols.HASH), prefix); // prefix newly created by base
        prefixMap.put(PrefixMap.getURI("sh"), "sh"); // prefix for sh:

        shapes = new TreeSet<>();
    }

    public Set<Shape> getShapes() { return shapes; }

    public String getRelativeIRIOr(final URI uri) {
        return getRelativeIRIOr(uri.toString());
    }

    public String getRelativeIRIOr(final String absoluteIRIString) {
        String relativeIRI = null;

        Set<Map.Entry<URI, String>> entrySet = prefixMap.entrySet();

        for (Map.Entry<URI, String> entry: entrySet) {
            String uri = entry.getKey().toString();
            if (absoluteIRIString.startsWith(uri)) {
                relativeIRI = absoluteIRIString.replace(uri, entry.getValue() + Symbols.COLON);

                if (isRelativeIRI(relativeIRI)) return relativeIRI;
            }
        }

        if (absoluteIRIString.startsWith(baseIRI.toString()))
            return Symbols.LT + absoluteIRIString.substring(absoluteIRIString.length()) + Symbols.GT;

        return Symbols.LT + absoluteIRIString + Symbols.GT;
    }

    private boolean isRelativeIRI(String relativeIRI) {
        if (relativeIRI.contains(Symbols.SLASH) || relativeIRI.contains(Symbols.HASH))
            return false;

        return true;
    }

    public String getSerializedPropertyShape(URI propertyShapeID) {
        for (Shape shape : shapes)
            if (shape instanceof PropertyShape) {
                PropertyShape propertyShape = (PropertyShape) shape;
                if (propertyShape.getID().equals(propertyShapeID))
                    return propertyShape.toString();
            }

        return null;
    }

    Optional<NodeShape> getMappedNodeShape(String mappedTableName) {
        Optional<NodeShape> mappedNodeShape = Optional.empty();

        for (Shape shape: shapes) {
            if (shape instanceof NodeShape) {
                NodeShape nodeShape = (NodeShape) shape;
                Optional<TableIRI> mappedTableIRI = nodeShape.getMappedTableIRI();
                if (mappedTableIRI.isPresent()) {
                    if (mappedTableIRI.get().getMappedTableName().equals(mappedTableName)) {
                        mappedNodeShape = Optional.of(nodeShape);
                        break;
                    }
                }
            }
        }

        return mappedNodeShape;
    }

    public NodeShape getMappedNodeShape(URI triplesMap) {
        for (Shape shape: shapes) {
            if (shape instanceof NodeShape) {
                NodeShape nodeShape = (NodeShape) shape;
                Optional<URI> mappedTriplesMap = nodeShape.getMappedTriplesMap();
                if (mappedTriplesMap.isPresent()) {
                    if (mappedTriplesMap.get().equals(triplesMap))
                        return nodeShape;
                }
            }
        }

        return null;
    }

    String getPrefixOf(URI uri) {
        Set<Map.Entry<URI, String>> entrySet = prefixMap.entrySet();

        for (Map.Entry<URI, String> entry: entrySet) {
            String key = entry.getKey().toString();
            if (uri.toString().startsWith(key))
                return entry.getValue();
        }

        return null;
    }

    void addShape(Shape shape) { shapes.add(shape); }

    void addPrefixDecl(String prefix, String IRIString) {
        prefixMap.put(URI.create(IRIString), prefix);
    }

    public Map<URI, String> getPrefixMap() { return prefixMap; }

    Optional<URI> getNamespaceIRI(String namespacePrefix) {
        Optional<URI> namespaceIRI = Optional.empty();
        Set<URI> keySet = prefixMap.keySet();
        for(URI key: keySet) {
            if (prefixMap.get(key).equals(namespacePrefix)) {
                namespaceIRI = Optional.of(key);
                return namespaceIRI;
            }
        }

        return namespaceIRI;
    }

    public URI getBaseIRI() {
        return baseIRI;
    }
    public String getPrefix() { return prefix; }

    Set<NodeShape> getNodeShapesOfSameSubject(NodeShape nodeShape) {
        Set<NodeShape> equivalentNodeShapes = new TreeSet<>();

        for (Shape existingShape: shapes) {
            if (existingShape instanceof PropertyShape) continue;

            NodeShape existingNodeShape = (NodeShape) existingShape;

            if (!existingNodeShape.getNodeKind().equals(nodeShape.getNodeKind())) continue;

            if (existingNodeShape.getRegex().equals(nodeShape.getRegex()))
                equivalentNodeShapes.add(existingNodeShape);
        }

        return equivalentNodeShapes;
    }

    // The cardinality of each element returned subset is greater than 1.
    Set<Set<NodeShape>> getSubsetOfPowerSetOf(Set<NodeShape> baseNodeShapes) {
        List<NodeShape> baseNodeShapeList = new ArrayList<>(baseNodeShapes);
        int size = baseNodeShapeList.size();

        Set<Set<NodeShape>> setsForDerivedNodeShapes = new HashSet<>();

        for (int i = 0; i < (1 << size); i++) {
            int oneBitsCount = Integer.bitCount(i);
            if (Integer.bitCount(i) > 1) {
                Set<NodeShape> set = new TreeSet<>();
                StringBuffer binaryString = new StringBuffer(Integer.toBinaryString(i)).reverse();
                for (int j = 0, fromIndex = 0; j < oneBitsCount; j++) {
                    fromIndex = binaryString.indexOf("1", fromIndex);
                    set.add(baseNodeShapeList.get(fromIndex++));
                }
                setsForDerivedNodeShapes.add(set);
            }
        }

        return setsForDerivedNodeShapes;
    }
}
