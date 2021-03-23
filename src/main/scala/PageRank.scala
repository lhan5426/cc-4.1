import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.PairRDDFunctions

object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      outputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {
      val sc = spark.sparkContext
      
      val lines = spark.read.textFile(inputGraphPath).rdd
      val links = lines.map{ s => 
          val parts = s.split("\\s+")
          (parts(0), parts(1))
      }.distinct().groupByKey()

      //unpack vertices from edges into Iterable[String]
      val vertices = links.values.flatMap(a=> a).
                     distinct()
      //map String:Iterable for join
      val nvertices = vertices.map(a => (a, vertices))
      
      //vertices.take(1).foreach(println)
      
      //val missing = vertices.subtract(links.keySet)
      val missing = nvertices.leftOuterJoin(links).map{ a => (a._1,a._2.getOrElse(links(a._1)))}
      val newlinks = (missing ++ links).cache()
      
      var ranks = missing.mapValues(v => 1.0/(vertices.count))
      //var ranks = newlinks.mapValues(v => 1.0/(vertices.size))
      
      for (i <- 1 to iterations) {
          val contribs = newlinks.join(ranks).
          values.
          flatMap{
              case (urls, rank) => 
              val size = urls.size
              urls.map(url => (url, rank/size))
          }
          ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)
      }

      //val output = ranks.collect()
      //output.foreach(tup => println(s"${tup._1} has rank: ${tup._2} ."))
      /*
      val top100 = sc.textFile(inputPath).
            map(_.split("\t").map(_.toInt)). // parse input into rdd of list(a,b)
            map(a => (a(1), 1)).                   // parse list(a,b)  to tuple(a,b).
            reduceByKey((x,y) => x + y).
            takeOrdered(100)(Ordering[Int].reverse.on(x => x._2))
      */
    
      val ranks_enc = ranks.
            map{case (a,b) => "%s\t%s" format (a,b)}
      ranks_enc.saveAsTextFile(outputPath)
      
      /*
      val pee_enc = vertices.
            map{case a => "%s\t" format a}
      pee_enc.saveAsTextFile("owo.txt")
      */
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val pageRankOutputPath = args(1)

    calculatePageRank(inputGraph, pageRankOutputPath, PageRankIterations, spark)
  }
}
