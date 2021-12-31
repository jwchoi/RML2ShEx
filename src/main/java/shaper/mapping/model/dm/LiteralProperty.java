package shaper.mapping.model.dm;

import shaper.mapping.Symbols;
import shaper.mapping.model.Utils;

import java.net.URI;

public class LiteralProperty implements Comparable<LiteralProperty> {
	private URI literalPropertyIRI;
	private String propertyFragment;
	
	private String mappedTable;
	private String mappedColumn;

	LiteralProperty(URI baseIRI, String mappedTable, String mappedColumn) {
		this.mappedTable = mappedTable;
		this.mappedColumn = mappedColumn;

        propertyFragment = buildLiteralPropertyFragment(mappedTable, mappedColumn);
        literalPropertyIRI = buildLiteralProperty(baseIRI, propertyFragment);
	}
	
	public String getMappedTable() {
		return mappedTable;
	}
	
	public String getMappedColumn() {
		return mappedColumn;
	}
	
	String getPropertyFragment() {
		return propertyFragment;
	}

	private String buildLiteralPropertyFragment(String table, String column) {
		return Utils.encode(table) + Symbols.HASH + Utils.encode(column);
	}

	private URI buildLiteralProperty(URI baseIRI, String propertyFragment) {
		return URI.create(baseIRI + propertyFragment);
	}

	public URI getLiteralPropertyIRI() { return literalPropertyIRI; }

	@Override
	public int compareTo(LiteralProperty o) {
		return literalPropertyIRI.compareTo(o.getLiteralPropertyIRI());
	}

	@Override
	public String toString() {
		return getLiteralPropertyIRI().toString();
	}
}
