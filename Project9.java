//***************************************************************
//
//  Developer:    Marshal Pfluger
//
//  Project #:    Project Nine
//
//  File Name:    Project9.java
//
//  Course:       COSC 3365 Distributed Databases Using Hadoop 
//
//  Due Date:     12/03/2023
//
//  Instructor:   Prof. Fred Kumi 
//
//  Description:  uses Spark to analyze advertising data from a file
//
//***************************************************************
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;



public class Project9 {
	
	//***************************************************************
    //
    //  Method:       main
    // 
    //  Description:  The main method of the program
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public static void main(String[] args) {
    	Project9 obj = new Project9();
    	obj.developerInfo();
        obj.advertisingData();
    }

	//***************************************************************
    //
    //  Method:       advertisingData
    // 
    //  Description:  runs the Spark program
    //
    //  Parameters:   N/A
    //
    //  Returns:      N/A 
    //
    //**************************************************************
    public void advertisingData() {
    	
    	// Suppress the warnings to only get important info
    	Logger.getLogger("org.apache").setLevel(Level.WARN);

    	// Create spark Conf
		SparkConf sparkConf = null;
		try {
			sparkConf = new SparkConf().setAppName("Sorted Sucess Rate").setMaster("local[*]");
			sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // Create the spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Set the input file and create RDD with it
        JavaRDD<String> inputFile = sparkContext.textFile("Project9.txt");

        // Convert the input data into a pair RDD with key as a tuple of category and location
        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> categoryLocationRDD = inputFile
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    Tuple2<String, String> categoryLocation = new Tuple2<>(parts[3], parts[2]);
                    Tuple2<Integer, Integer> clicksSales = new Tuple2<>(Integer.parseInt(parts[4]), Integer.parseInt(parts[5]));
                    return new Tuple2<>(categoryLocation, clicksSales);
                });

        // Calculate success rate for each category by location
        JavaPairRDD<Tuple2<String, String>, Double> successRatesRDD = categoryLocationRDD
                .mapValues(data -> ((double) data._2() / data._1()) * 100);

        // Use reduceByKey to calculate the sum and count for each category by location
        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> sumCountRDD = successRatesRDD
                .mapValues(rate -> new Tuple2<>(rate, 1))
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1() + t2._1(), t1._2() + t2._2()));
        
        // Calculate average success rate for each category by location
        JavaPairRDD<Tuple2<String, String>, Double> avgSuccessRatesRDD = sumCountRDD
                .mapValues(data -> data._1() / data._2());

        // Swap key and value for sorting by success rate
        JavaPairRDD<Double, Tuple2<String, String>> swappedSortedRDD = avgSuccessRatesRDD.mapToPair(Tuple2::swap).sortByKey(false);

        
        // Swap key and value for sorting by success rate
        JavaPairRDD<Tuple2<String, String>, Double> sortedFinalRDD = swappedSortedRDD.mapToPair(Tuple2::swap);
        
        // Collect and print the results
        sortedFinalRDD.collect().forEach(tuple -> System.out.println(String.format("%-12s%-14s%.2f%%", tuple._1._1, tuple._1._2, tuple._2)));

        // Uncomment to send output to a file
        //sortedFinalRDD.saveAsTextFile("advertisingData"); 
        
        // Close sparkContext
        sparkContext.close();
    }

    //***************************************************************
    //
    //  Method:       developerInfo (Non Static)
    // 
    //  Description:  The developer information method of the program
    //                This method must be included in all projects.
    //
    //  Parameters:   None
    //
    //  Returns:      N/A
    //
    //***************************************************************
	public void developerInfo()
	{
		System.out.println("Name:    Marshal Pfluger");
		System.out.println("Course:  COSC 3365 Distributed Databases Using Hadoop");
		System.out.println("Program: Nine\n");
		} // End of the developerInfo method
}