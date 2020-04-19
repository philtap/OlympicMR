import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 *
 * @author PHILIPPET
 */

public class OlympicMR1 {


    public static class MedalMapper extends Mapper<LongWritable, Text, Text, Text> {

//        The mapper Receives one record of the summer medals file (summer_medals.csv) at a time
//        (a key, a line of text)
//         Year     City        Sport       Discipline  Athlete          Country  Gender  Event         Medal   OlympicEvent                       EventGender
//         1992	    Barcelona	Basketball	Basketball	Jordan, Michael	 USA	  Men	  Basketball	Gold    Basketball-Basketball-Basketball   Men
//
//        It needs to split the record, and return
//         - Key: the Year
//         - Value: a String composed of the Country then Event,Gender,then Medal
//            Year         Country,EventGender,Medal
//            1992	       USA,Basketball-Basketball-Basketball-Men,Gold




        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Get a row/line from the file
            String line = value.toString();

            // Split it into fields
            String[] field = line.split(",");

            // Get year
            String year = field[0];

            // Get country code
            String country = field[5];

            String olympicEvent = field[9];

            String eventGender = field[10];

            String medal = field[8];

            String event = olympicEvent + "-" + eventGender;

            String countryResult = country + "," + event + "," + medal;

            if (!country.equals("Country")) {
                // This is not the header row, write
                context.write(new Text("Year-" + year), new Text(countryResult));
            }
        }
    }

    public static class MedalReducer
            extends Reducer<Text, Text, Text, Text>
    {

        private MultipleOutputs<Text, Text> multipleOutputs;

        protected void setup( Context context ) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {

            // For each call of the reduce function :
            // Input:
            // Key: Ranking type - Year
            // Iterable: list of all participant medals: Country, Event_Gender, Medal
            // Key: 2012
            // Iterable: USA, Basketball-Basketball-Basketball-Men , Gold

            // 3 Output files each containing all countries ranked with a different system
            // GF ranking
            // 1992  Gold First Ranking
            // 	Country	  Gold	    Silver	    Bronze
            //  EUN         45          38          29
            //  USA	        37	        34	        37
            //  GER         33          21          28
            //  ...

            // 1992  Sum Ranking
            // Country	  Total
            //  EUN         112
            //  USA	        108
            //  GER          82
            //

            // 1992  Weight Ranking
            // Country	  Weight (4:2:1)
            //  EUN          285
            //  USA	         253
            //  GER          202
            //

            String keyString = key.toString();
            String[] header = keyString.split("-");

            String rankingSystem ;

            String year = header[1];
            System.out.println("-----------------------------------");
            System.out.println("MedalReducer-year:" + year);
            System.out.println("-----------------------------------");

            // hashMap will contain a medal per event and Gender (Men,Women,Mixed)
            Map<String, Integer> medalCounter = new HashMap<String, Integer>();

            // -------------------------------------------------------------------
            // Part 1 - List the actual (distinct) medals
            //--------------------------------------------------------------------
            // Loop through each line (a participant medal) and check if the medal was already counted
            // in the hash map
            //
            for (Text value : values) {
                // Split the text into Event and Result
                String valueString = value.toString();
                String[] field = valueString.split(",");
                String country = field[0];
                String event = field[1];

                // This is the type of medal : Bronze, ....
                String medal = field[2];

                // countryEventMedal example: USA_Basketball-Basketball-Basketball-Men_Gold
                String countryEventMedal = country + "_" + event + "_" + medal;

                // Important, it is possible that participants from 2 countries get the same medal
                //  if they obtain the same result e.g same time or distance etc...
                // That's ok as 2 different reducers will be used
                // Also it is possible that 2 separate participants from the same country get the same result
                // Also in fighting events (Boxing, Judo), all semi finalists get a Bronze medal
                // TO DO : is that covered atm??

                if (!medalCounter.containsKey(countryEventMedal)) {
                    //System.out.println( "MedalReducer-new medal to count");
                   // System.out.println("MedalReducer - countryEventMedal=" + countryEventMedal);
                    medalCounter.put(countryEventMedal, 1);
                }
            }

            // --------------------------------------------------------------
            // Part 2 -Add up the medals per country
            // --------------------------------------------------------------

            // Count of medals for all Countries for an Olympic Year

            //  Here , we start with a hashmap containing all distinct medals for an Olympic year
            //  USA_Basketball-Basketball-Basketball-Men_Gold  1
            //  USA_Basketball-Basketball-Basketball-Women_Silver  1
            //  Need to sum up the medals for each country for this year using the previous hashMap
            //  Output should be an unordered list
            //  Country	   totalGold	totalSilver	    totalBronze     totalMedal       SumPoints
            //   USA	   37	        34	            37                 108              285
            //   GER       33           21              28                 82               253
            //   EUN       45           38              29                 112              202

            // Use a hashmap of Objects (olympicResult) , each containing the results for a country
            // for the current year being processed:
            // Country ,  ( totalGold,totalSilver,totalBronze,totalMedal,SumPoints)

            Map<String, olympicResult> countryMedals = new HashMap<String, olympicResult>();

            int gold, silver, bronze, total;
            double sum;

            for (String country_event_medal : medalCounter.keySet()) {

                String[] field = country_event_medal.split("_");
                String country = field[0];
                String medal_type = field[2];

                // Initialise each count
                    gold = 0;
                    silver = 0;
                    bronze = 0;
                    total = 0;
                    sum = 0.0;

                // Calculate the contribution of the new medal
                switch (medal_type) {
                    case "Gold":
                        gold = 1;
                        total = 1;
                        sum = 1.72;
                        break;
                    case "Silver":
                        //System.out.println("MedalMapper-Silver");
                        silver = 1;
                        total = 1;
                        sum = 0.86;
                        break;
                    case "Bronze":
                        //System.out.println("MedalReducer-Bronze");
                        bronze = 1;
                        total = 1;
                        sum = 0.43;
                        break;
                }


                if(countryMedals.containsKey(country)) {
                    // The country exists already in the hashmap, update it
                    olympicResult currentResult = countryMedals.get(country);

                    currentResult.setTotalGold(currentResult.getTotalGold() + gold);
                    currentResult.setTotalSilver( currentResult.getTotalSilver() + silver);
                    currentResult.setTotalBronze(currentResult.getTotalBronze() + bronze);
                    currentResult.setTotalMedal (currentResult.getTotalMedal() + total);
                    double sumPoints  = Math.round(( currentResult.getSumPoints() + sum )* 100.0) / 100.0;
                    currentResult.setSumPoints (sumPoints) ;
                    countryMedals.put(country,currentResult );
                }
                else {
                    // The country doesn't exist in the hashmap, add it
                    countryMedals.put(country, new olympicResult(gold, silver, bronze, total, sum));
                }
            }



            //-----------------------------------------------------------------------------------------------
            // Part 3 - Sort the medals per country using the Sum Ranking System (i.e total medals)
            //-----------------------------------------------------------------------------------------------
             // Now let's sort the HashMap by values
            // There is no direct way to sort HashMap by values but it can be
            // achieved by writing a besopke comparator, which takes 2 Map Entry object a
            // and arrange them in descending order
            //

            rankingSystem = "SR";

            Set<Entry<String, olympicResult>> entries = countryMedals.entrySet();

             Comparator<Entry<String, olympicResult>> valueComparator = new Comparator<Entry<String, olympicResult>>()
             {
                    @Override
                    // Compare 2 olympicResult objects
                    public int compare(Entry<String, olympicResult> e1, Entry<String, olympicResult> e2) {
                        olympicResult r1 = e1.getValue();
                        olympicResult r2 = e2.getValue();
                        Integer totalMedal1 = r1.getTotalMedal();
                        Integer totalMedal2 = r2.getTotalMedal();
                        // Descending order
                        return totalMedal2.compareTo(totalMedal1);
                    }
             };

              // Sort method needs a List, so let's first convert Set to List in Java
              List<Entry<String, olympicResult>> listOfEntries = new ArrayList<Entry<String, olympicResult>>(entries);

              // Sorting HashMap by values using comparator
              Collections.sort(listOfEntries, valueComparator);

              LinkedHashMap<String, olympicResult> sortedByValue = new LinkedHashMap<String, olympicResult>(listOfEntries.size());

              // Copying entries from List to Map
              for (Entry<String, olympicResult> entry : listOfEntries)
              {
                    sortedByValue.put(entry.getKey(), entry.getValue());
              }

              Set<Entry<String, olympicResult>> entrySetSortedByValue = sortedByValue.entrySet();

               String rankingSystemStr = "Sum Ranking";
               String filename = "SumRanking";

               multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
               multipleOutputs.write(new Text(year), new Text(rankingSystemStr) , filename);
               multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
               multipleOutputs.write(new Text("Rank    Country"), new Text("     Medals") , filename);
               multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""),filename);


                int rank =0;
                String rankStr , countryStr, totalMedalStr;

                // Ordered list of countries
                for (Entry<String, olympicResult> mapping : entrySetSortedByValue) {
                    rank ++;
                    String countryCode = mapping.getKey();
                    int yearMedals = mapping.getValue().getTotalMedal();

                    //Formatting of countryCode over 10 characters
                    countryStr = formatOutput.formatStr(countryCode,10);
                    // Formatting of Rank over 10 characters
                    rankStr= formatOutput.formatNum(rank,10);
                    totalMedalStr = formatOutput.formatNum(yearMedals,10);

                    System.out.println( rankStr + countryStr+ totalMedalStr);
                    multipleOutputs.write(new Text(rankStr + countryStr), new Text(totalMedalStr), filename);
                }


            //-----------------------------------------------------------------------------------------------
            // Part 4 - Sort the medals per country by the Weight Ranking System (4:2:1 weight)
            // -----------------------------------------------------------------------------------------------

            rankingSystem = "WR";

            // Use same sorting algorithm as above

            entries = countryMedals.entrySet();

            valueComparator = new Comparator<Entry<String, olympicResult>>()
            {
                    @Override
                    // Compare 2 olympicResult objects
                    public int compare(Entry<String, olympicResult> e1, Entry<String, olympicResult> e2) {
                        olympicResult r1 = e1.getValue();
                        olympicResult r2 = e2.getValue();
                        Double sumPoints1 = r1.getSumPoints();
                        Double sumPoints2 = r2.getSumPoints();
                        // Descending order
                        return sumPoints2.compareTo(sumPoints1);
                    }
             };

            listOfEntries = new ArrayList<Entry<String, olympicResult>>(entries);

             // Sorting HashMap by values using comparator
             Collections.sort(listOfEntries, valueComparator);

             //LinkedHashMap<String, olympicResult> sortedByValue = new LinkedHashMap<String, olympicResult>(listOfEntries.size());
             sortedByValue = new LinkedHashMap<String, olympicResult>(listOfEntries.size());

             // Copying entries from List to Map
             for (Entry<String, olympicResult> entry : listOfEntries)
             {
                    sortedByValue.put(entry.getKey(), entry.getValue());
             }
             //System.out.println("HashMap after sorting entries by values ");
              //Set<Entry<String, olympicResult>> entrySetSortedByValue = sortedByValue.entrySet();
             entrySetSortedByValue = sortedByValue.entrySet();

             rankingSystemStr = "Weight Ranking";
             filename = "WeightRanking";
             String totalSumPointsStr;

             // Header for the Year
             multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
             multipleOutputs.write(new Text(year), new Text(rankingSystemStr) , filename);
             multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
             multipleOutputs.write(new Text("Rank    Country"), new Text("     Points") , filename);
             multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""),filename);

            rank =0;
              // Ordered list of countries
             for (Entry<String, olympicResult> mapping : entrySetSortedByValue)
             {
                   rank ++;
                   String countryCode = mapping.getKey();
                   double yearSumPoints= mapping.getValue().getSumPoints();

                   //Formatting of countryCode over 10 characters
                   countryStr = formatOutput.formatStr(countryCode,10);
                   // Formatting of Rank over 10 characters
                   rankStr= formatOutput.formatNum(rank,10);
                   totalSumPointsStr = formatOutput.formatNum(yearSumPoints,10);

                   System.out.println( rankStr + countryStr+ totalSumPointsStr );
                   multipleOutputs.write(new Text(rankStr + countryStr), new Text(totalSumPointsStr ), filename);

             }

            //-----------------------------------------------------------------------------------------------
            // Part 5 - Sort the medals per country by the Gold First Ranking System
            // -----------------------------------------------------------------------------------------------
            rankingSystem = "GFR";

            // Use same sorting algorithm as above

            entries = countryMedals.entrySet();

            valueComparator = new Comparator<Entry<String, olympicResult>>()
            {
                @Override
                // Compare 2 olympicResult objects, here using Gold First ranking
                public int compare(Entry<String, olympicResult> e1, Entry<String, olympicResult> e2) {
                    String countryCode1 = e1.getKey();
                    String countryCode2 = e2.getKey();
                    olympicResult r1 = e1.getValue();
                    olympicResult r2 = e2.getValue();
                    Integer TotalGold1 = r1.getTotalGold();
                    Integer TotalSilver1 = r1.getTotalSilver();
                    Integer TotalBronze1 = r1.getTotalBronze();
                    Integer TotalGold2 = r2.getTotalGold();
                    Integer TotalSilver2 = r2.getTotalSilver();
                    Integer TotalBronze2 = r2.getTotalBronze();
                    //Compare Gold medals first between the 2 countries
                    // if equal compare Silver, if equal compare Bronze
                    // If all medals are equal, sort alphabetically
                    // Descending order
                    if (TotalGold1 != TotalGold2) {
                        // descending order
                        return TotalGold2.compareTo(TotalGold1);
                    }
                    else
                     {
                        if (TotalSilver1 != TotalSilver2)
                        {
                            // descending order
                            return TotalSilver2.compareTo(TotalSilver1);
                        }
                        else
                        {
                            if (TotalBronze1 != TotalBronze2)
                            {
                                // descending order
                                return TotalBronze2.compareTo(TotalBronze1);
                            }
                            else
                             {
                                // ascending order
                                return countryCode1.compareTo(countryCode2);
                            }

                        }

                    }
                }
            };

            // Sort method needs a List, so let's first convert Set to List in Java
            //List<Map.Entry<String, olympicResult>> listOfEntries = new ArrayList<Map.Entry<String, olympicResult>>(entries);

            listOfEntries = new ArrayList<Entry<String, olympicResult>>(entries);

            // Sorting HashMap by values using comparator
            Collections.sort(listOfEntries, valueComparator);

            //LinkedHashMap<String, olympicResult> sortedByValue = new LinkedHashMap<String, olympicResult>(listOfEntries.size());
            sortedByValue = new LinkedHashMap<String, olympicResult>(listOfEntries.size());

            // Copying entries from List to Map
            for (Entry<String, olympicResult> entry : listOfEntries)
            {
                sortedByValue.put(entry.getKey(), entry.getValue());
            }
            //System.out.println("HashMap after sorting entries by values ");
            //Set<Entry<String, olympicResult>> entrySetSortedByValue = sortedByValue.entrySet();
            entrySetSortedByValue = sortedByValue.entrySet();

            rankingSystemStr = "Gold First Ranking";
            filename = "GoldFirstRanking";

            // Header for the Year
            multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
            multipleOutputs.write(new Text(year), new Text(rankingSystemStr) , filename);
            multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
            multipleOutputs.write(new Text("Rank    Country"), new Text("     Gold      Silver    Bronze") , filename);
            multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""),filename);

            rank =0;
            for (Entry<String, olympicResult> mapping : entrySetSortedByValue)
            {
                rank ++;
                String countryCode = mapping.getKey();
                int yearTotalGold= mapping.getValue().getTotalGold();
                int yearTotalSilver=mapping.getValue().getTotalSilver();
                int yearTotalBronze=mapping.getValue().getTotalBronze();

                //Formatting of countryCode over 10 characters
                countryStr = formatOutput.formatStr(countryCode,10);
                // Formatting of Rank over 10 characters
                rankStr= formatOutput.formatNum(rank,10);
                String totalGoldStr = formatOutput.formatNum(yearTotalGold,10);
                String totalSilverStr = formatOutput.formatNum(yearTotalSilver,10);
                String totalBronzeStr = formatOutput.formatNum(yearTotalBronze,10);

                System.out.println( rankStr + countryStr+ totalGoldStr +  totalSilverStr + totalBronzeStr);
                multipleOutputs.write(new Text(rankStr + countryStr), new Text(totalGoldStr +  totalSilverStr + totalBronzeStr) , filename);

            }
        }

     }

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: MROlympic <input dir> <output dir>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(OlympicMR1.class);
        job.setJobName("MapReduce Olympic 1");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedalMapper.class);
        job.setReducerClass(MedalReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



