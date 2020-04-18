import org.apache.commons.lang.StringUtils;

public class formatOutput {

    public static String formatNum (int inputNum, int n )
    {
        String formatted  = Integer.toString(inputNum);
        formatted = StringUtils.leftPad(formatted, 3, " ");
        formatted = StringUtils.rightPad(formatted, n, " ");
        return formatted;
    }
    public static String formatNum (double inputNum, int n )
    {
        String formatted  = Double.toString(inputNum);
        formatted = StringUtils.leftPad(formatted, 3, " ");
        formatted = StringUtils.rightPad(formatted, n, " ");
        return formatted;
    }
    public static String formatStr (String inputStr, int n)
    {
        return StringUtils.rightPad(inputStr, n, " ");
    }
}
