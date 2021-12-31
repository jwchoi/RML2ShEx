package shaper.mapping.model.dm;

import janus.database.DBField;
import shaper.mapping.Symbols;

import java.net.URI;
import java.util.List;

public class BlankNode {
    // generates a blank node with ordered column names.
    static String getMappedBlankNodeFragment(String table, List<DBField> fields) {
        StringBuffer blankNodeFragment = new StringBuffer(Symbols.UNDERSCORE);
        blankNodeFragment.append(Symbols.COLON);

        blankNodeFragment.append(table);

        String rowNodeAfterBase = RowNode.getMappedRowNodeAfterBase(table, fields);
        URI rowNodeIRI = RowNode.getRowNode(rowNodeAfterBase);
        int hash = rowNodeIRI.hashCode();

        blankNodeFragment.append(hash);

        return blankNodeFragment.toString();
    }
}
