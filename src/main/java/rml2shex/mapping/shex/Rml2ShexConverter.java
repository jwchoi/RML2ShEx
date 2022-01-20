package rml2shex.mapping.shex;

import rml2shex.model.shex.DeclarableShapeExpr;
import rml2shex.util.Symbols;
import rml2shex.model.rml.*;
import rml2shex.model.shex.ShExDocModel;
import rml2shex.model.shex.ShExDocModelFactory;
import rml2shex.mapping.rml.RMLParser;

import java.io.File;
import java.io.PrintWriter;
import java.net.URI;
import java.util.*;

public class Rml2ShexConverter {
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

    private RMLParser getRMLParser() { return new RMLParser(rmlPathname, RMLParser.Lang.TTL); }

    private void writeDirectives() {
        // base
        writer.println(Symbols.BASE + Symbols.SPACE + Symbols.LT + shExDocModel.getBaseIRI() + Symbols.GT);

        // prefixes
        Set<Map.Entry<URI, String>> entrySet = shExDocModel.getPrefixMap().entrySet();
        for (Map.Entry<URI, String> entry: entrySet)
            writer.println(Symbols.PREFIX + Symbols.SPACE + entry.getKey() + Symbols.COLON + Symbols.SPACE + Symbols.LT + entry.getValue() + Symbols.GT);

        writer.println();
    }

    private void writeShEx() {
        Set<DeclarableShapeExpr> declarableShapeExprs = shExDocModel.getDeclarableShapeExprs();

        declarableShapeExprs.stream().forEach(declarableShapeExpr -> writer.println(declarableShapeExpr.getShapeExprDecl()));
        declarableShapeExprs.stream().forEach(declarableShapeExpr -> System.out.println(declarableShapeExpr.getShapeExprDecl()));
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
        shExDocModel = ShExDocModelFactory.getShExDocModel(rmlModel, shexBasePrefix, shexBaseIRI);

        preProcess();
        writeDirectives();
        writeShEx();
        postProcess();

        return output;
    }
}
