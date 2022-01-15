package rml2shex.util;

import java.net.URI;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class PrefixMap {
	private static Map<URI, String> map = new Hashtable<>();
	
	static {
		map.put(URI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns#"), "rdf");
		map.put(URI.create("http://www.w3.org/2001/XMLSchema#"), "xsd");
		map.put(URI.create("http://www.w3.org/2004/02/skos/core"), "skos");
		map.put(URI.create("http://www.w3.org/XML/1998/namespace"), "xml");
		map.put(URI.create("http://www.w3.org/2000/01/rdf-schema#"), "rdfs");
		map.put(URI.create("http://www.w3.org/2002/07/owl"), "owl");
		map.put(URI.create("http://www.w3.org/ns/shacl#"), "sh");
		map.put(URI.create("http://www.w3.org/ns/r2rml#"), "rr");
	}

	public static URI getURI(String prefix) {
		Set<Map.Entry<URI, String>> entrySet = map.entrySet();

		Iterator<Map.Entry<URI, String>> iterator = entrySet.iterator();

		while (iterator.hasNext()) {
			Map.Entry<URI, String> entry = iterator.next();

			if (entry.getValue().equals(prefix))
				return entry.getKey();
		}

		return null;
	}
}