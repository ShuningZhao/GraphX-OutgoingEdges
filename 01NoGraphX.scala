package sparktoys.nographx

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object Problem1 {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val outputFolder = args(1)
      val conf = new SparkConf().setAppName("ReverseGraph").setMaster("local")
      val sc = new SparkContext(conf)
      val input = sc.textFile(inputFile)
      val outEdges = input.map(line => line.split("[\t\\s]+")).map(x => (x(1), (x(3).toDouble,1)))
      val nodeList = outEdges.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(x=> (x._1, x._2._1.toDouble/x._2._2)).filter(a => a._2 > 0)
      val result = nodeList.sortBy( x=> (-x._2, x._1.toInt))
      val outputFormat = result.map(x => x._1 +"\t" + x._2)
      outputFormat.saveAsTextFile(outputFolder)
    }
}