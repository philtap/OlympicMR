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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author PHILIPPET
 */

public class OlympicMR2 {
    // --------------------------------------
    // This is version 2.0 of the code
    // --------------------------------------


    public static class MedalMapper extends Mapper<LongWritable, Text, Text, Text> {

//        Receives one record of the summer athletes file (summer_athletes.csv) ) at a time
//        (a key, a line of text)
//          ID	    Name	                    Sex	    Team	    Country	Year	Sport	    Event	                                    Medal   EventType   EntryName
//          55424	Earvin "Magic" Johnson Jr.	M	    United States	USA	1992	Basketball	Basketball Men's Basketball	                Gold     Team       United States
//          55881	Michael Jeffrey Jordan	    M	    United States	USA	1992	Basketball	Basketball Men's Basketball	                Gold     Team       United States
//          2424	Santiago Aldama Aleson      M	    Spain	        ESP	1992	Basketball	Basketball Men's Basketball	                NA       Team       United States
//          58163   Risako Kawai                F       Japan           JPN 1992    Wrestling   Wrestling Women's Middleweight Freestyle    Gold     Individual Risako Kawai
//        it needs to split the record, and return
//         - Key: the Year
//         - Value: a String composed of :
//            Country	        Event 	                                    Medal     EventType       EntryName
//            USA               Basketball Men's Basketball	                Gold       Team           United States
//            USA               Basketball Men's Basketball	                Gold       Team           United States
//            ESP               Basketball Men's Basketball	                NA         Team           Spain
//            JPN               Wrestling Women's Middleweight Freestyle    Gold       Individual     Risako Kawai
////

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 0       1       2     3       4        5       6       7       8         9           10
            // ID    Name    Sex    Team     Country Year   Sport   Event   Medal   EventType   EntryName

            //System.out.println( "MedalMapper-Start");

            String line = value.toString();
            //System.out.println( "MedalMapper-line="+ line);

            String[] field = line.split(",");

            // Get year
            String year = field[5];
            //System.out.println("MedalMapper-year=" + year);
            // Get country code
            String country = field[4];
           //System.out.println( "MedalMapper-country="+ country);

            String event = field[7];
            //System.out.println( "MedalMapper-event="+ event);

            // May not be needed: EntryType could be enough
            String eventType = field[9];
            //System.out.println( "MedalMapper-event="+ eventType);

            String entryName = field[10];
            //System.out.println( "MedalMapper-event="+ entryName);

            String medal = field[8];
            //System.out.println( "MedalMapper-medal="+ medal);

            String athleteEntry = country + "," + event + "," + eventType + "," + entryName + ","  +  medal ;
            //System.out.println("MedalMapper-" + year + ":" + athleteEntry);

            if (!country.equals("Country"))
            {
                context.write(new Text( year), new Text(athleteEntry));
            }
            else
            {
               //System.out.println("MedalMapper-skip header line");

            }

            //System.out.println( "MedalMapper - End ");
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
            // Key: the Year
            // Iterable : list of all participant athletes, for each a String composed of :
//            Country	   Event 	                                 EventType       EntryName         Medal
//            USA          Basketball Men's Basketball	               Team           United States    Gold
//            USA          Basketball Men's Basketball	               Team           United States    Gold
//            ESP          Basketball Men's Basketball	               Team           Spain            NA
//            JPN          Wrestling Women's Middleweight Freestyle   Individual      Risako Kawai     Gold

            // 2 Output files each containing all countries ranked using 2 different systems
            // sum percentage ranking
            // 1992         Sum Percentage Ranking
            // 	Country
            //  EUN         0.25
            //  USA	        0.18
            //  GER         0.12
            //  ...
            // Weight percentage ranking
            // 1992         Weight Percentage Ranking
            // Country
            //  EUN         0.8
            //  USA	        0.45
            //  GER         0.35


            String year = key.toString();

            String rankingSystem ;

            System.out.println("-----------------------------------");
            System.out.println("MedalReducer-year:" + year);
            System.out.println("-----------------------------------");

            // hashMap will contain each distinct entry for a country to an event (individual or team)

            Map<String, String> entryCounter = new HashMap<String, String>();

            // -------------------------------------------------------------------
            // Part 1 - List the unique/distinct entries
            //--------------------------------------------------------------------
            // This step should provide the a result in this format
            // Country  Event                                       EntryName           Participants:Medal
            // USA      Basketball Men's Basketball	                United States       13:Gold
            // ESP      Basketball Men's Basketball	                Spain               13:NA
            // JPN      Wrestling Women's Middleweight Freestyle    Risako Kawai        1:Gold
            //
            //
            // Loop through each line (a participant) and check if an entry was already recorded in the hashmap
            // If No, create a new entry with athletes = 1 and result for the entry: NA, Bronze, Silver, Gold
            // If yes: add 1 to the athletes count for this entry

            for (Text value : values)
            {
                // Split the text into Event and Result
                String valueString = value.toString();
                String[] field = valueString.split(",");
                String country = field[0];
                String event = field[1];
                String entryName = field[3];
                String medal = field[4];

                // Unique identifier of an entry is Country, event, entryName
                // USA          Basketball Men's Basketball	               United States  (one team entry repeated for each member of the team)
                // ESP          Basketball Men's Basketball	               Spain           (same)
                // JPN          Wrestling Women's Middleweight Freestyle   Risako Kawai    (one individual entry for a Wrestling event)

                String countryEventEntry= country + "_" + event + "_" + entryName ;

                // Check if this entry was already recorded or not
                if (!entryCounter.containsKey(countryEventEntry))
                {
                    // This is a new entry
                    String entryValue = "1:" + medal ;
                    entryCounter.put(countryEventEntry, entryValue);
                }
                else
                {
                    // This entry already exists, extract current data
                    String entryValue = entryCounter.get(countryEventEntry);
                    String[] val = entryValue.split(":");
                    // Increment count of athletes by 1
                    int athletes = Integer.parseInt(val[0])	+ 1  ;
                    // Keep the result
                    medal = val[1];
                    entryCounter.put(countryEventEntry, athletes + ":" + medal)  ;
                }
            }

//            // Print result of part 1
            for (String countryEventEntry : entryCounter.keySet()) {
                entryCounter.get(countryEventEntry);
                //System.out.println(countryEventEntry+"="+entryCounter.get(countryEventEntry));
            }

            // -------------------------------------------------------------------------
            // Part 2 - Add up the entries and medals per country for the Olympic year
            // --------------------------------------------------------------------------

            //  Here , we start with a hashmap containing all distinct entries for an Olympic year
            // Country  Event                                       EntryName           Participants:Medal
            // USA      Basketball Men's Basketball	                United States       13:Gold
            // ESP      Basketball Men's Basketball	                Spain               13:NA
            // JPN      Wrestling Women's Middleweight Freestyle    Risako Kawai        1:Gold

//          //  Need to sum up the entries and medals for each country for this year using the previous hashMap
//          //  Output should be an unordered list
//          //  Country	   Participants entries  totalGold	    totalSilver	    totalBronze         totalMedal          sumPoints
//          //   USA	      550          350           37	          34	          37                 108              285
//          //   GER          350          320           33           21              28                 82               253
//          //   EUN          480          340           45           38              29                 112              202
//
//            // Use a hashmap of Objects (olympicResult) , each containing the results for a country
//            // for the current year being processed:
            // Country ,  ( totalGold,totalSilver,totalBronze,totalMedal,SumPoints,Participants entries)
            Map<String, olympicResult> countryResults = new HashMap<String, olympicResult>();

            int gold, silver, bronze, total, numEntries, participants;
            double sum;
            // Loop through the previous hashmap of distinct event entries
            for (String country_event_entry : entryCounter.keySet())
            {

                // Get the hashmap key
                String[] field = country_event_entry.split("_");
                String country = field[0];
                String event = field[1];
                String entryName = field[1];

                // Get the hashmap value
                String entryValue = entryCounter.get(country_event_entry );
                String[] field2 = entryValue.split(":");
                String medal_type = field2[1];

                // Initialise each count for this entry, this will apply for NA
                participants = Integer.parseInt(field2[0]);
                gold = 0;
                silver = 0;
                bronze = 0;
                total = 0;
                sum = 0.0;
                numEntries = 1;

                // Calculate the contribution of the new entry to medal counts
                switch (medal_type) {
                    case "Gold":
                        gold = 1;
                        total = 1;
                        sum = 4;
                        sum = 1.72;
                        break;
                    case "Silver":
                        silver = 1;
                        total = 1;
                        sum=2;
                        sum = 0.86;
                        break;
                    case "Bronze":
                        bronze = 1;
                        total = 1;
                        sum = 1;
                        sum = 0.43;
                        break;
                }

                // Check if the country exists in the countryResults hashmap
                if(countryResults.containsKey(country))
                {

                    // The country exists already in the hashmap, update it
                    olympicResult currentResult = countryResults.get(country);

                    currentResult.setTotalParticipants(currentResult.getTotalParticipants() + participants);
                    currentResult.setTotalEntries(currentResult.getTotalEntries() + numEntries);
                    currentResult.setTotalGold(currentResult.getTotalGold() + gold);
                    currentResult.setTotalSilver( currentResult.getTotalSilver() + silver);
                    currentResult.setTotalBronze(currentResult.getTotalBronze() + bronze);
                    currentResult.setTotalMedal (currentResult.getTotalMedal() + total);
                    currentResult.setSumPoints (currentResult.getSumPoints() + sum) ;
                    countryResults.put(country,currentResult );
                }
                else
                {
                    // The country doesn't exist in the hashmap, add it
                    countryResults.put(country, new olympicResult(gold, silver, bronze, total, sum, participants, numEntries));
                }

            }


            olympicResult Result;

//          Temp - Print result of part 2
//
            for (String countryRes: countryResults.keySet())
            {

                entryCounter.get(countryRes);
                Result = countryResults.get(countryRes);
                //System.out.println( year + ":" + countryRes + ":" + Result.toString());
            }

            // -------------------------------------------------------------------------
            // Part 3 - Calculate the sum percentage and weight percentage
            // --------------------------------------------------------------------------
            // Now that we have all information per country for the year, calculate the 2 scores
            // for each country

            double totalMedals, totalEntries, sumPoints;

            for (String countryRes: countryResults.keySet())
            {
                entryCounter.get(countryRes);
                Result = countryResults.get(countryRes);
                totalEntries = Result.getTotalEntries();
                totalMedals= Result.getTotalMedal();
                sumPoints = Result.getSumPoints();

                //Round sumPoints to 2 decimals
                sumPoints  = Math.round(( sumPoints  )* 100.0) / 100.0;

                // Round to 3 decimals
                double SumPercentage = Math.round(( totalMedals/ totalEntries )* 1000.0) / 1000.0;
                double WeightPercentage = Math.round(( sumPoints / totalEntries )* 1000.0) / 1000.0;

                Result.setSumPercentage ( SumPercentage  );
                Result.setWeightPercentage (WeightPercentage );
                Result.setSumPoints (sumPoints );

                //System.out.println( year + ":" + countryRes + ":" + Result.toString());


            }


            //-----------------------------------------------------------------------------------------------
            // Part 4 - Sort the medals per country using the Sum percentage (percentage of medals per entry)
            //-----------------------------------------------------------------------------------------------
             // Sort the HashMap by values
            // There is no direct way to sort HashMap by values but it can be
            // achieved by writing a bespoke comparator, which takes 2 Map Entry object a
            // and arrange them in descending order
            //

            rankingSystem = "SP";

            Set<Entry<String, olympicResult>> entries = countryResults.entrySet();

             Comparator<Entry<String, olympicResult>> valueComparator = new Comparator<Entry<String, olympicResult>>()
             {
                    @Override
                    // Compare 2 olympicResult objects based on Sum Percentage
                    public int compare(Entry<String, olympicResult> e1, Entry<String, olympicResult> e2) {
                        olympicResult r1 = e1.getValue();
                        olympicResult r2 = e2.getValue();
                        Double SumPercentage1 = r1.getSumPercentage();
                        Double SumPercentage2 = r2.getSumPercentage();
                        // Descending order
                        return SumPercentage2.compareTo(SumPercentage1);
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

              //System.out.println("HashMap after sorting entries by values ");
              Set<Entry<String, olympicResult>> entrySetSortedByValue = sortedByValue.entrySet();

              String rankingSystemStr = "Sum Percentage";
              String filename = "SumPercentage";

                // Header for the Year
              multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
              multipleOutputs.write(new Text(year), new Text(rankingSystemStr) , filename);
              multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
              multipleOutputs.write(new Text("Rank    Country"), new Text("     Medals   Entries       Score") , filename);
              multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);

              // Header for the Year
              System.out.println("-------------------------------------------------");
              System.out.println( year +"    "+ rankingSystemStr);
              System.out.println("-------------------------------------------------");
              System.out.println("Rank    Country     Medals   Entries       Score") ;
              System.out.println("-------------------------------------------------");


                int rank =0;
                String rankStr , countryStr, totalMedalStr, totalEntriesStr ;

                // Ordered list of countries will be output by the Reducer in descending order
                // of their Weight Percentage, prefixed with their ranking
                for (Entry<String, olympicResult> mapping : entrySetSortedByValue)
                {
                    rank ++;
                    String countryCode = mapping.getKey();
                    int yearMedals = mapping.getValue().getTotalMedal();
                    int yearEntries = mapping.getValue().getTotalEntries();
                    String yearSumPercentage = Double.toString(mapping.getValue().getSumPercentage());

                    //Formatting of countryCode over 10 characters
                    countryStr = formatOutput.formatStr(countryCode,10);
                    // Formatting of Rank over 10 characters
                    rankStr= formatOutput.formatNum(rank,10);
                    totalMedalStr = formatOutput.formatNum(yearMedals,10);
                    totalEntriesStr = formatOutput.formatNum(yearEntries,10);

                    System.out.println( rankStr + countryStr+ totalMedalStr+totalEntriesStr+yearSumPercentage);
                    multipleOutputs.write(new Text(rankStr + countryStr), new Text(totalMedalStr+totalEntriesStr+yearSumPercentage), filename);
                }
            //-----------------------------------------------------------------------------------------------
            // Part 4 - Sort the medals per country using the Weight percentage (percentage of medals per entry)
            //-----------------------------------------------------------------------------------------------
            // Sort the HashMap by values
            // There is no direct way to sort HashMap by values but it can be achieved by writing a bespoke comparator,
            // which takes 2 Map Entry object and arranges them in descending order
            //

            rankingSystem = "WP";

            valueComparator = new Comparator<Entry<String, olympicResult>>()
            {
                @Override
                // Compare 2 olympicResult objects based on Weight Percentage
                public int compare(Entry<String, olympicResult> e1, Entry<String, olympicResult> e2) {
                    olympicResult r1 = e1.getValue();
                    olympicResult r2 = e2.getValue();
                    Double WeightPercentage1 = r1.getWeightPercentage();
                    Double WeightPercentage2 = r2.getWeightPercentage();
                    // Descending order
                    return WeightPercentage2.compareTo(WeightPercentage1);
                }
            };

            // Sort method needs a List, so let's first convert Set to List in Java
            listOfEntries = new ArrayList<Entry<String, olympicResult>>(entries);

            // Sorting HashMap by values using comparator
            Collections.sort(listOfEntries, valueComparator);

            sortedByValue = new LinkedHashMap<String, olympicResult>(listOfEntries.size());

            // Copying entries from List to Map
            for (Entry<String, olympicResult> entry : listOfEntries)
            {
                sortedByValue.put(entry.getKey(), entry.getValue());
            }

            //System.out.println("HashMap after sorting entries by values ");
            entrySetSortedByValue = sortedByValue.entrySet();

            rankingSystemStr = "Weight Percentage";
            filename = "WeightPercentage";

            // Header for the Year
            multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
            multipleOutputs.write(new Text(year), new Text(rankingSystemStr) , filename);
            multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);
            multipleOutputs.write(new Text("Rank    Country"), new Text("     Points   Entries       Score") , filename);
            multipleOutputs.write(new Text("-------------------------------------------------"), new Text(""), filename);

            // Header for the Year
            System.out.println("-------------------------------------------------");
            System.out.println( year +"    "+ rankingSystemStr);
            System.out.println("-------------------------------------------------");
            System.out.println("Rank    Country     Points   Entries       Score") ;
            System.out.println("-------------------------------------------------");

            // Calculate the rank of each country
            rank =0;
            String totalSumPointsStr;

            // Ordered list of countries
            for (Entry<String, olympicResult> mapping : entrySetSortedByValue)
            {
                rank ++;
                String countryCode = mapping.getKey();

                double yearSumPoints= mapping.getValue().getSumPoints();
                int yearEntries = mapping.getValue().getTotalEntries();
                String yearWeightPercentage = Double.toString(mapping.getValue().getWeightPercentage());

                //System.out.println(countryCode + " : " + yearWeightPercentage);

                //Formatting of all outputs to 10 characters
                countryStr = formatOutput.formatStr(countryCode,10);
                rankStr= formatOutput.formatNum(rank,10);
                totalSumPointsStr = formatOutput.formatNum(yearSumPoints,10);
                totalEntriesStr = formatOutput.formatNum(yearEntries,10);

                System.out.println( rankStr + countryStr+ totalSumPointsStr+totalEntriesStr+yearWeightPercentage);
                multipleOutputs.write(new Text(rankStr + countryStr), new Text(totalSumPointsStr+totalEntriesStr+yearWeightPercentage), filename);
            }
        }

     }

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Usage: MROlympic2 <input dir> <output dir>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(OlympicMR2.class);
        job.setJobName("MapReduce Olympic 2");

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



