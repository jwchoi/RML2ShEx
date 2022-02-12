package rml2shex.model.rml;

import rml2shex.datasource.Database;
import rml2shex.datasource.Service;

import java.net.URI;
import java.util.Optional;

public class Source {
    private URI source; // CSV, JSON, XML

    private Optional<Database> database; // database
    private Optional<Service> service; // SPARQL Endpoint

    Source(URI source) {
        this.source = source;
        database = Optional.empty();
        service = Optional.empty();
    }

    Source(String source) { this(URI.create(source)); }

    public URI getSource() { return source; }

    public Optional<Database> getDatabase() { return database; }
    void setDatabase(Optional<Database> database) { this.database = database; }

    public Optional<Service> getService() { return service; }
    void setService(Optional<Service> service) { this.service = service; }
}
