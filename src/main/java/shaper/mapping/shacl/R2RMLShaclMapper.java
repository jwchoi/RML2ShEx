package shaper.mapping.shacl;

import shaper.Shaper;
import shaper.mapping.Symbols;
import shaper.mapping.model.r2rml.R2RMLModel;
import shaper.mapping.model.r2rml.R2RMLModelFactory;
import shaper.mapping.model.shacl.NodeShape;
import shaper.mapping.model.shacl.ShaclDocModelFactory;
import shaper.mapping.model.shacl.Shape;
import shaper.mapping.r2rml.R2RMLParser;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class R2RMLShaclMapper extends ShaclMapper {
    private String R2RMLPathname;

    public R2RMLShaclMapper(String R2RMLPathname) { this.R2RMLPathname = R2RMLPathname; }

    private R2RMLParser getR2RMLParser() { return new R2RMLParser(R2RMLPathname, R2RMLParser.Lang.TTL); }

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

    private void preProcess() {
        String fileName = R2RMLPathname.substring(R2RMLPathname.lastIndexOf("/")+1, R2RMLPathname.lastIndexOf("."));
        fileName = fileName + Symbols.DASH + "SHACL";
        output = new File(Shaper.DEFAULT_DIR_FOR_SHACL_FILE + fileName + "." + "ttl");

        try {
            writer = new PrintWriter(output);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public File generateShaclFile() {
        R2RMLModel r2rmlModel = R2RMLModelFactory.getR2RMLModel(getR2RMLParser());
        shaclDocModel = ShaclDocModelFactory.getSHACLDocModel(r2rmlModel);

        preProcess();
        writeDirectives();
        writeShacl();
        postProcess();

        System.out.println("Translating the R2RML into SHACL has finished.");

        return output;
    }
}
