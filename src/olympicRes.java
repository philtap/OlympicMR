public class olympicRes {
        public static void main(String[] args) throws Exception {

            if (args.length > 0) {
                //System.out.println("Usage: MROlympic <input dir> <output dir>");
                System.exit(-1);
            }

            olympicResult myRes = new olympicResult(10,20,30,60,110.0);
            System.out.println( myRes.toString());

            olympicResult myRes2 = new olympicResult(10,20,30,60,110.0,500,300);
            System.out.println( myRes2.toString());

            olympicResult myRes3 = new olympicResult(10,20,30,60,110.0,500,300,0.2,0.36);
            System.out.println( myRes3.toString());

        }
    }

