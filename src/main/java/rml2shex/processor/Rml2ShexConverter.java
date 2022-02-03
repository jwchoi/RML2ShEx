package rml2shex.processor;

import rml2shex.datasource.MetadataExtractor;
import rml2shex.model.shex.DeclarableShapeExpr;
import rml2shex.commons.Symbols;
import rml2shex.model.rml.*;
import rml2shex.model.shex.ShExDocModel;
import rml2shex.model.shex.ShExDocModelFactory;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.*;

public class Rml2ShexConverter {
    private String dataSourceDir;

    private String rmlPathname;
    private String shexPathname;

    private String shexBasePrefix;
    private URI shexBaseIRI;

    private ShExDocModel shExDocModel;

    private File output;
    private PrintWriter writer;

    public Rml2ShexConverter(String rmlPathname, String shexPathname, String shexBasePrefix, String shexBaseIRI) {
        this.rmlPathname = rmlPathname;
        this.shexPathname = shexPathname;
        this.shexBasePrefix = shexBasePrefix;
        this.shexBaseIRI = URI.create(shexBaseIRI);
    }

    public Rml2ShexConverter(String dataSourceDir, String rmlPathname, String shexPathname, String shexBasePrefix, String shexBaseIRI) {
        this.dataSourceDir = dataSourceDir;
        this.rmlPathname = rmlPathname;
        this.shexPathname = shexPathname;
        this.shexBasePrefix = shexBasePrefix;
        this.shexBaseIRI = URI.create(shexBaseIRI);
    }

    private RMLParser getRMLParser() { return new RMLParser(rmlPathname, RMLParser.Lang.TTL); }

    private void writeDirectives() {
        // prefixes
        Set<Map.Entry<URI, String>> entrySet = shExDocModel.getPrefixMap().entrySet();
        for (Map.Entry<URI, String> entry: entrySet)
            writer.println(Symbols.PREFIX + Symbols.SPACE + entry.getValue() + Symbols.COLON + Symbols.SPACE + Symbols.LT + entry.getKey() + Symbols.GT);

        // base
        writer.println(Symbols.BASE + Symbols.SPACE + Symbols.LT + shExDocModel.getBaseIRI() + Symbols.GT);

        writer.println();
    }

    private void writeShEx() {
        Set<DeclarableShapeExpr> declarableShapeExprs = shExDocModel.getDeclarableShapeExprs();

        declarableShapeExprs.stream().sorted().forEach(declarableShapeExpr -> writer.println(declarableShapeExpr.getSerializedShapeExpr() + Symbols.NEWLINE));
        declarableShapeExprs.stream().sorted().forEach(declarableShapeExpr -> System.out.println(declarableShapeExpr.getSerializedShapeExpr() + Symbols.NEWLINE));
    }

    private void preProcess() {
        output = new File(shexPathname);

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

    public File generateShExFile() {
        RMLModel rmlModel = RMLModelFactory.getRMLModel(getRMLParser());
        if (dataSourceDir != null) MetadataExtractor.injectMetadataInto(rmlModel, dataSourceDir);
        shExDocModel = ShExDocModelFactory.getShExDocModel(rmlModel, shexBasePrefix, shexBaseIRI);

        preProcess();
        writeDirectives();
        writeShEx();
        postProcess();

        return output;
    }
}
