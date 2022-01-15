package rml2shex.mapping.model.rml;

import java.net.URI;

public class Service {
    private URI uri;

    private URI endpoint;
    private URI supportedLanguage;
    private URI resultFormat;

    Service(URI uri, URI endpoint, URI supportedLanguage, URI resultFormat) {
        this.uri = uri;

        this.endpoint = endpoint;
        this.supportedLanguage = supportedLanguage;
        this.resultFormat = resultFormat;
    }
}
