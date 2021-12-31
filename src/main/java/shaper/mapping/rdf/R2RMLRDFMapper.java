package shaper.mapping.rdf;

import shaper.Shaper;
import shaper.mapping.Symbols;

import java.io.File;
import java.io.PrintWriter;

public class R2RMLRDFMapper extends RDFMapper {
    private String R2RMLPathname;

    public R2RMLRDFMapper(String R2RMLPathname) { this.R2RMLPathname = R2RMLPathname; }

    private void preProcess() {
        String fileName = R2RMLPathname.substring(R2RMLPathname.lastIndexOf("/")+1, R2RMLPathname.lastIndexOf("."));
        fileName = fileName + Symbols.DASH + "R2RML";
        output = new File(Shaper.DEFAULT_DIR_FOR_RDF_FILE + fileName + "." + "ttl");

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
    public File generateRDFFile() {
        preProcess();
        postProcess();

        System.out.println("R2RML Mapping is not supported yet, so an empty file is generated.");
        return output;
    }
}
