package rml2shex.util;

import rml2shex.util.Symbols;

public class Utils {
    public static String encode(String value) {
        return value.replace(Symbols.SPACE, "%20");
    }
}
