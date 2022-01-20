package rml2shex.model.rml;

import java.net.URI;

public class Source {
    private URI source;

    Source(String source) { this.source = URI.create(source); }
    Source(URI source) { this.source = source; }
}
