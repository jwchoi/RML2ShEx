package rml2shex.commons;

public class Utils {
    public static String encode(String value) {
        return value.replace(Symbols.SPACE, "%20");
    }
}
