package shaper.mapping.model.shex;

import shaper.mapping.Symbols;
import shaper.mapping.model.r2rml.ObjectMap;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public class ShExSchema {
    private URI baseIRI;
    private String prefix;

    private Map<URI, String> prefixMap;

    private Set<Shape> shapes;
    private Set<NodeConstraint> nodeConstraints;

    private Set<ShapeExpr> shapeExprs;

    ShExSchema(URI baseIRI, String prefix) {
        this.baseIRI = baseIRI;
        this.prefix = prefix;

        prefixMap = new TreeMap<>();
        prefixMap.put(URI.create(baseIRI + Symbols.HASH), prefix); // prefix newly created by base

        shapes = new CopyOnWriteArraySet<>();
        nodeConstraints = new CopyOnWriteArraySet<>();
    }

    void addShape(Shape shape) { shapes.add(shape); }

    void addNodeConstraint(NodeConstraint nodeConstraint) {
        nodeConstraints.add(nodeConstraint);
    }

    public String getMappedShapeID(String table) {
        for (Shape shape: shapes)
            if (shape.getMappedTableName().equals(table))
                return shape.getShapeID();

        return null;
    }

    public String getMappedShape(String table) {
        for (Shape shape: shapes)
            if (shape.getMappedTableName().equals(table))
                return shape.toString();

        return null;
    }

    public String getMappedNodeConstraintID(String table, String column) {
        for (NodeConstraint nodeConstraint : nodeConstraints)
            if (nodeConstraint.getMappedTable().equals(table)
                    && nodeConstraint.getMappedColumn().equals(column))
                return nodeConstraint.getNodeConstraintID();

        return null;
    }

    public String getMappedNodeConstraint(String table, String column) {
        for (NodeConstraint nodeConstraint : nodeConstraints)
            if (nodeConstraint.getMappedTable().equals(table)
                    && nodeConstraint.getMappedColumn().equals(column))
                return nodeConstraint.toString();

        return null;
    }

    public String getMappedNodeConstraint(ObjectMap objectMap) {
        for (NodeConstraint nodeConstraint : nodeConstraints)
            if (nodeConstraint.getMappedObjectMap().equals(objectMap))
                return nodeConstraint.toString();

        return null;
    }

    public String getMappedNodeConstraintID(ObjectMap objectMap) {
        for (NodeConstraint nodeConstraint : nodeConstraints)
            if (nodeConstraint.getMappedObjectMap().equals(objectMap))
                return nodeConstraint.getNodeConstraintID();

        return null;
    }

    public URI getBaseIRI() {
        return baseIRI;
    }
    public String getPrefix() { return prefix; }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Shape getMappedShape(URI triplesMap) {
        for (Shape shape: shapes) {
            Optional<URI> mappedTriplesMap = shape.getMappedTriplesMap();
            if (mappedTriplesMap.isPresent()) {
                if (mappedTriplesMap.get().equals(triplesMap))
                    return shape;
            }
        }

        return null;
    }

    Set<Shape> getShapesToShareTheSameSubjects(Shape shape) {
        Set<Shape> equivalentShapes = new CopyOnWriteArraySet<>();

        if (!shape.getNodeKind().equals(NodeKinds.IRI))
            return equivalentShapes;

        for (Shape existingShape: shapes)
            if (existingShape.getNodeKind().equals(NodeKinds.IRI) && existingShape.getRegex().equals(shape.getRegex()))
                equivalentShapes.add(existingShape);

        return equivalentShapes;
    }

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

    public Set<Shape> getDerivedShapes() {
        Set<Shape> derivedShapes = new CopyOnWriteArraySet<>();

        for (Shape shape: shapes) {
            if (!shape.getMappedTriplesMap().isPresent())
                derivedShapes.add(shape);
        }

        return derivedShapes;
    }

    Set<Shape> getDerivedShapesFrom(Set<Shape> baseShapes) {
        Set<Shape> derivedShapes = new CopyOnWriteArraySet<>();

        for (Shape baseShape: baseShapes) {
            for (Shape shape: shapes) {
                if (derivedShapes.contains(shape)) continue;
                if (!shape.getMappedTriplesMap().isPresent()) {
                    if (shape.containsInBaseShapes(baseShape))
                        derivedShapes.add(shape);
                }
            }
        }

        return derivedShapes;
    }
}
