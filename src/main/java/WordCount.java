import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WordCount {

    /*For Simplicity,
     *We are creating custom Split function,so it makes code easier to understand
     *We are implementing FlatMapFunction interface.*/
    static class SplitFunction implements FlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterator<String> call(String s) {
            return Arrays.asList(s.split(" ")).iterator();
        }

    }

    public static void main(String[] args) throws AnalysisException {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Spark Performance Test").master("local[*]").getOrCreate();

        Dataset<Row> dataset1 = sparkSession.read().option("header", true).option("inferSchema", true)
                .csv("C:\\Users\\45961\\PycharmProjects\\TxtFileGenerator\\*.csv");
        dataset1.createTempView("dataset1");

        Dataset<Row> dataset2 = sparkSession.read().option("header", true).option("inferSchema", true)
                .csv("C:\\Users\\45961\\PycharmProjects\\TxtFileGenerator\\*.csv");

        Dataset<Row> result = sparkSession.sql("select * from dataset1");
        result.show();


    }
}