package shaper.mapping.model.shex;

import shaper.mapping.Symbols;
import shaper.mapping.model.r2rml.ObjectMap;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

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
        Optional<Shape> mappedShape = shapes.stream()
                .filter(shape -> shape instanceof DMShape)
                .filter(shape -> ((DMShape) shape).getMappedTableName().equals(table))
                .findAny();

        return mappedShape.isEmpty() ? null : mappedShape.get().getShapeID();
    }

    public String getMappedShape(String table) {
        Optional<Shape> mappedShape = shapes.stream()
                .filter(shape -> shape instanceof DMShape)
                .filter(shape -> ((DMShape) shape).getMappedTableName().equals(table))
                .findAny();

        return mappedShape.isEmpty() ? null : mappedShape.get().toString();
    }

    public String getMappedNodeConstraintID(String table, String column) {
        Optional<DMNodeConstraint> mappedNodeConstraint = nodeConstraints.stream()
                .filter(nc -> nc instanceof DMNodeConstraint)
                .map(nc -> (DMNodeConstraint) nc)
                .filter(nc -> nc.getMappedTable().equals(table) && nc.getMappedColumn().equals(column))
                .findAny();

        return mappedNodeConstraint.isPresent() ? mappedNodeConstraint.get().getNodeConstraintID() : null;
    }

    public String getMappedNodeConstraint(String table, String column) {
        Optional<DMNodeConstraint> mappedNodeConstraint = nodeConstraints.stream()
                .filter(nc -> nc instanceof DMNodeConstraint)
                .map(nc -> (DMNodeConstraint) nc)
                .filter(nc -> nc.getMappedTable().equals(table) && nc.getMappedColumn().equals(column))
                .findAny();

        return mappedNodeConstraint.isPresent() ? mappedNodeConstraint.get().toString() : null;
    }

    public String getMappedNodeConstraint(ObjectMap objectMap) {
        Optional<R2RMLNodeConstraint> nodeConstraint = nodeConstraints.stream()
                .filter(nc -> nc instanceof R2RMLNodeConstraint)
                .map(nc -> (R2RMLNodeConstraint) nc)
                .filter(nc -> nc.getMappedObjectMap().equals(objectMap))
                .findAny();

        return nodeConstraint.isPresent() ? nodeConstraint.get().toString() : null;
    }

    public String getMappedNodeConstraintID(ObjectMap objectMap) {
        Optional<R2RMLNodeConstraint> nodeConstraint = nodeConstraints.stream()
                .filter(nc -> nc instanceof R2RMLNodeConstraint)
                .map(nc -> (R2RMLNodeConstraint) nc)
                .filter(nc -> nc.getMappedObjectMap().equals(objectMap))
                .findAny();

        return nodeConstraint.isPresent() ? nodeConstraint.get().getNodeConstraintID() : null;
    }

    public URI getBaseIRI() {
        return baseIRI;
    }
    public String getPrefix() { return prefix; }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Shape getMappedShape(URI triplesMap) {
        Optional<Shape> mappedShape = shapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .filter(shape -> ((R2RMLShape) shape).getMappedTriplesMap().isPresent())
                .filter(shape -> ((R2RMLShape) shape).getMappedTriplesMap().get().equals(triplesMap))
                .findAny();

        return mappedShape.isEmpty() ? null : mappedShape.get();
    }

    Set<Shape> getShapesToShareTheSameSubjects(Shape shape) {
        if (!(shape instanceof R2RMLShape)) return Collections.EMPTY_SET;

        R2RMLShape r2RMLShape = (R2RMLShape) shape;

        if (!r2RMLShape.getNodeKind().equals(NodeKinds.IRI))
            return Collections.EMPTY_SET;

        return shapes.stream()
                .filter(e -> e instanceof R2RMLShape)
                .filter(e -> ((R2RMLShape) e).getNodeKind().equals(NodeKinds.IRI)
                        && ((R2RMLShape) e).getRegex().equals(r2RMLShape.getRegex()))
                .collect(Collectors.toSet());
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
        return shapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .filter(shape -> ((R2RMLShape) shape).getMappedTriplesMap().isEmpty())
                .collect(Collectors.toSet());
    }

    Set<Shape> getDerivedShapesFrom(Set<Shape> baseShapes) {
        Set<Shape> derivedShapes = new CopyOnWriteArraySet<>();

        Set<R2RMLShape> shapes = this.shapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .map(shape -> (R2RMLShape) shape)
                .collect(Collectors.toSet());

        Set<R2RMLShape> r2rmlBaseShapes = baseShapes.stream()
                .filter(shape -> shape instanceof R2RMLShape)
                .map(shape -> (R2RMLShape) shape)
                .collect(Collectors.toSet());

        for (R2RMLShape baseShape: r2rmlBaseShapes) {
            for (R2RMLShape shape: shapes) {
                if (derivedShapes.contains(shape)) continue;

                if (shape.getMappedTriplesMap().isEmpty()) {
                    if (shape.containsInBaseShapes(baseShape))
                        derivedShapes.add(shape);
                }
            }
        }

        return derivedShapes;
    }
}
