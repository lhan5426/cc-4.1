import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}
import org.apache.spark.sql.functions._

object FollowerDF {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path in parquet format.
    *
    * It must be done using the DataFrame/Dataset API.
    *
    * It is NOT valid to do it with the RDD API, and convert the result to a DataFrame, nor to read
    * the graph as an RDD and convert it to a DataFrame.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param spark the spark session.
    */
  def computeFollowerCountDF(inputPath: String, outputPath: String, spark: SparkSession): Unit = {
    // TODO: Calculate the follower count for each user
    // TODO: Write the top 100 users to the above outputPath in parquet format
    val schema = new StructType().
                    add("Follower", IntegerType).
                    add("Followee", IntegerType)
    val df = spark.read.format("csv").
            option("delimiter", "\t").
            schema(schema).
            load("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")
    val counts = df.groupBy("Followee").count().
                orderBy(desc("count")).
                limit(100)
    counts.write.parquet(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val followerDFOutputPath = args(1)

    computeFollowerCountDF(inputGraph, followerDFOutputPath, spark)
  }

}
