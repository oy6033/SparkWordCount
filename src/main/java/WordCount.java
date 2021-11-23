import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
public class WordCount {

    /*For Simplicity,
     *We are creating custom Split function,so it makes code easier to understand
     *We are implementing FlatMapFunction interface.*/
    static class SplitFunction implements FlatMapFunction<String, String>
    {
        private static final long serialVersionUID = 1L;
        @Override
        public Iterator<String> call(String s) {
            return Arrays.asList(s.split(" ")).iterator();
        }

    }

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Spark WordCount example using Java");

        //Setting Master for running it from IDE.
        sparkConf.setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        /*Reading input file whose path was specified as args[0]*/
        JavaRDD<String> textFile = sparkContext.textFile("/home/micheal/Desktop/opt/spark/word.txt");

        /*Creating RDD of words from each line of input file*/
        JavaRDD<String> words = textFile.flatMap(new SplitFunction());

        /*Below code generates Pair of Word with count as one
         *similar to Mapper in Hadoop MapReduce*/
        JavaPairRDD<String, Integer> pairs = words
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

        /*Below code aggregates Pairs of Same Words with count
         *similar to Reducer in Hadoop MapReduce
         */
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) Integer::sum);
        /*Saving the result file to the location that we have specified as args[1]*/
        counts.saveAsTextFile("opt/spark/");
        sparkContext.stop();
        sparkContext.close();
    }
}