package shaper.mapping.shex;

import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.rml.ObjectMap;
import shaper.mapping.model.rml.PredicateObjectMap;
import shaper.mapping.model.rml.RMLModelFactory;
import shaper.mapping.model.rml.TriplesMap;
import shaper.mapping.model.shex.ShExSchemaFactory;
import shaper.mapping.model.shex.Shape;
import shaper.mapping.rml.RMLParser;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RMLShExMapper extends ShExMapper {
    private String RMLPathname;

    public RMLShExMapper(String RMLPathname) { this.RMLPathname = RMLPathname; }

    private RMLParser getRMLParser() { return new RMLParser(RMLPathname, RMLParser.Lang.TTL); }

    private void writeDirectives() {
        // base
        writer.println(Symbols.BASE + Symbols.SPACE + Symbols.LT + shExSchema.getBaseIRI() + Symbols.GT);

        // prefix for newly created shape expressions
        writer.println(Symbols.PREFIX + Symbols.SPACE + shExSchema.getBasePrefix() + Symbols.COLON + Symbols.SPACE + Symbols.LT + shExSchema.getBaseIRI() + Symbols.HASH + Symbols.GT);

        // prefixes
        Set<Map.Entry<String, String>> entrySet = rmlModel.getPrefixMap().entrySet();
        for (Map.Entry<String, String> entry: entrySet)
            writer.println(Symbols.PREFIX + Symbols.SPACE + entry.getKey() + Symbols.COLON + Symbols.SPACE + Symbols.LT + entry.getValue() + Symbols.GT);
        writer.println();
    }

    private void writeShEx() {
        Set<TriplesMap> triplesMaps = rmlModel.getTriplesMaps();

        for (TriplesMap triplesMap : triplesMaps) {
            List<PredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
            for (PredicateObjectMap predicateObjectMap: predicateObjectMaps) {
                List<PredicateObjectMap.PredicateObjectPair> predicateObjectPairs = predicateObjectMap.getPredicateObjectPairs();
                for (PredicateObjectMap.PredicateObjectPair predicateObjectPair: predicateObjectPairs) {
                    Optional<ObjectMap> objectMap = predicateObjectPair.getObjectMap();
                    if (objectMap.isPresent()) {
//                        if (NodeConstraint.isPossibleToHaveXSFacet(objectMap.get())) {
//                            String nodeConstraintID = shExSchema.getMappedNodeConstraintID(objectMap.get());
//                            String nodeConstraint = shExSchema.getMappedNodeConstraint(objectMap.get());
//                            if (nodeConstraintID != null && nodeConstraint != null) {
//                                String id = shExSchema.getPrefix() + Symbols.COLON + nodeConstraintID;
//                                writer.println(id + Symbols.SPACE + nodeConstraint);
//                            }
//                        }
                    }
                }
            }
            writer.println();

            writer.println(shExSchema.getMappedShape(triplesMap.getUri()));
            writer.println();
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        Set<Shape> derivedShapes = shExSchema.getDerivedShapes();
        for (Shape derivedShape: derivedShapes) {
            writer.println(derivedShape);
            writer.println();
        }
    }

    private void preProcess() {
        String fileName = RMLPathname.substring(RMLPathname.lastIndexOf("/")+1, RMLPathname.lastIndexOf("."));
        fileName = fileName + Symbols.DASH + "ShEx";
        output = new File(Shaper.DEFAULT_DIR_FOR_SHEX_FILE + fileName + "." + "shex");

        try {
            writer = new PrintWriter(output);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void postProcess() {
        try {
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public File generateShExFile() {
        rmlModel = RMLModelFactory.getRMLModel(getRMLParser());
        shExSchema = ShExSchemaFactory.getShExSchemaModel(rmlModel);

        preProcess();
        writeDirectives();
        writeShEx();
        postProcess();

        System.out.println("Translating the R2RML into ShEx has finished.");

        return output;
    }
}
