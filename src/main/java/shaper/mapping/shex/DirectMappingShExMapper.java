package shaper.mapping.shex;

import shaper.Shaper;
import shaper.mapping.PrefixMap;
import shaper.mapping.Symbols;
import shaper.mapping.model.dm.DirectMappingModelFactory;
import shaper.mapping.model.shex.ShExSchemaFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;

public class DirectMappingShExMapper extends ShExMapper {

    private void writeDirectives() {
        // base
        writer.println(Symbols.BASE + Symbols.SPACE + Symbols.LT + shExSchema.getBaseIRI() + Symbols.GT); // for an RDF Graph by direct mapping

        // prefix for newly created shape expressions
        writer.println(Symbols.PREFIX + Symbols.SPACE + shExSchema.getPrefix() + Symbols.COLON + Symbols.SPACE + Symbols.LT + shExSchema.getBaseIRI() + Symbols.HASH + Symbols.GT);

        // prefixID
        writer.println(Symbols.PREFIX + Symbols.SPACE + "rdf" + Symbols.COLON + Symbols.SPACE + Symbols.LT + PrefixMap.getURI("rdf") + Symbols.GT);
        writer.println(Symbols.PREFIX + Symbols.SPACE + "xsd" + Symbols.COLON + Symbols.SPACE + Symbols.LT + PrefixMap.getURI("xsd") + Symbols.GT);
        writer.println(Symbols.PREFIX + Symbols.SPACE + "rdfs" + Symbols.COLON + Symbols.SPACE + Symbols.LT + PrefixMap.getURI("rdfs") + Symbols.GT);
        writer.println();
    }

    private void writeShEx() {
        Set<String> tables = Shaper.dbSchema.getTableNames();

        for(String table : tables) {
            List<String> columns = Shaper.dbSchema.getColumns(table);
            for (String column: columns) {
                String id = shExSchema.getPrefix() + Symbols.COLON + shExSchema.getMappedNodeConstraintID(table, column);
                writer.println(id + Symbols.SPACE + shExSchema.getMappedNodeConstraint(table, column));
            }
            writer.println();

            writer.println(shExSchema.getMappedShape(table));
            writer.println();
        }
    }

    private void preProcess() {
        String catalog = Shaper.dbSchema.getCatalog();

        output = new File(Shaper.DEFAULT_DIR_FOR_SHEX_FILE + catalog + "." + "shex");

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
        directMappingModel = DirectMappingModelFactory.generateMappingModel(Shaper.dbSchema);
        shExSchema = ShExSchemaFactory.getShExSchemaModel(Shaper.dbSchema);

        preProcess();
        writeDirectives();
        writeShEx();
        postProcess();

        System.out.println("Translating the schema into ShEx has finished.");

        return output;
    }
}
