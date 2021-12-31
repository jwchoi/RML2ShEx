package shaper.mapping.shacl;

import shaper.mapping.model.shacl.ShaclDocModel;

import java.io.File;
import java.io.PrintWriter;

public abstract class ShaclMapper {
    protected ShaclDocModel shaclDocModel; // dependent on r2rmlModel or rdfMappingModel

    protected File output;
    protected PrintWriter writer;

    public abstract File generateShaclFile();
}
