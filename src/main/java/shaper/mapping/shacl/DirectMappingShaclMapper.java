package shaper.mapping.shacl;

import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.dm.DirectMappingModel;
import shaper.mapping.model.dm.DirectMappingModelFactory;
import shaper.mapping.model.shacl.NodeShape;
import shaper.mapping.model.shacl.ShaclDocModelFactory;
import shaper.mapping.model.shacl.Shape;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class DirectMappingShaclMapper extends ShaclMapper {

    private void preProcess() {
        String catalog = Shaper.dbSchema.getCatalog();

        output = new File(Shaper.DEFAULT_DIR_FOR_SHACL_FILE + catalog + "." + "ttl");

        try {
            writer = new PrintWriter(output);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeDirectives() {
        // base
        writer.println(Symbols.AT + Symbols.base + Symbols.SPACE + Symbols.LT + shaclDocModel.getBaseIRI() + Symbols.GT + Symbols.SPACE + Symbols.DOT);

        // prefixes
        Set<Map.Entry<URI, String>> entrySet = shaclDocModel.getPrefixMap().entrySet();
        for (Map.Entry<URI, String> entry: entrySet)
            writer.println(Symbols.AT + Symbols.prefix + Symbols.SPACE + entry.getValue() + Symbols.COLON + Symbols.SPACE + Symbols.LT + entry.getKey() + Symbols.GT + Symbols.SPACE + Symbols.DOT);
        writer.println();
    }

    private void writeShacl() {
        Set<Shape> shapes = shaclDocModel.getShapes();

        for (Shape shape: shapes) {
            if (shape instanceof NodeShape) {
                NodeShape nodeShape = (NodeShape) shape;

                Set<URI> propertyShapeIDs = nodeShape.getPropertyShapeIDs();
                for (URI propertyShapeID: propertyShapeIDs)
                    writer.println(shaclDocModel.getSerializedPropertyShape(propertyShapeID));

                writer.println(nodeShape);
            }
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
    public File generateShaclFile() {
        DirectMappingModel directMappingModel = DirectMappingModelFactory.generateMappingModel(Shaper.dbSchema);
        shaclDocModel = ShaclDocModelFactory.getSHACLDocModel(directMappingModel);

        preProcess();
        writeDirectives();
        writeShacl();
        postProcess();

        System.out.println("Translating the schema into SHACL has finished.");

        return output;
    }
}
