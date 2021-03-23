import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FollowerRDD {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path with userID and
    * count **tab separated**.
    *
    * It must be done using the RDD API.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param sc the SparkContext.
    */
  def computeFollowerCountRDD(inputPath: String, outputPath: String, sc: SparkContext): Unit = {
    // TODO: Calculate the follower count for each user
    // TODO: Write the top 100 users to the outputPath with userID and count **tab separated**
    val top100 = sc.textFile(inputPath).
            map(_.split("\t").map(_.toInt)). // parse input into rdd of list(a,b)
            map(a => (a(1), 1)).                   // parse list(a,b)  to tuple(a,b).
            reduceByKey((x,y) => x + y).
            takeOrdered(100)(Ordering[Int].reverse.on(x => x._2))
            
    val top100RDD = sc.parallelize(top100).
            map{case (a,b) => "%s\t%s" format (a,b)}
    top100RDD.saveAsTextFile(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputGraph = args(0)
    val followerRDDOutputPath = args(1)

    computeFollowerCountRDD(inputGraph, followerRDDOutputPath, sc)
  }
}
