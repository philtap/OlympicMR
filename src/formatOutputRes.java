public class formatOutputRes {

    public static void main(String[] args) throws Exception {

        if (args.length > 0) {
            System.exit(-1);
        }

        String s = formatOutput.formatNum(5,10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatNum(78,10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatNum(187,10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatStr("AUS",10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatStr("ABCDEF",10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatStr("A",10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatStr("",10);
        System.out.println( "|" + s + "|");

        s = formatOutput.formatNum(0.285,10);
        System.out.println( "|" + s + "|");


    }
}
