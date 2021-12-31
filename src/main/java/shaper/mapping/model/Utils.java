package shaper.mapping.model;

import shaper.mapping.Symbols;

public class Utils {
    public static String encode(String value) {
        return value.replace(Symbols.SPACE, "%20");
    }
}
