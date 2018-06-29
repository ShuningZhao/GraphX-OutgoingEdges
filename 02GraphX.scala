package sparktoys.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object Problem2 {
    def main(args: Array[String]) {
      val inputFile = args(0)
      val sourceNodeID = args(1)
      val conf = new SparkConf().setAppName("ReverseGraph").setMaster("local")
      val sc = new SparkContext(conf)
      val input = sc.textFile(inputFile)
      val edgeArray = input.map(line => line.split("[\t\\s]+")).map(x => Edge(x(1).toLong, x(2).toLong, x(3).toDouble)).collect()
      val verticesArray = input.map(line => line.split("[\t\\s]+")).map(x => (x(1).toLong, (x(1).toString))).distinct().collect().sorted
      val edgeRDD : RDD[Edge[Double]] = sc.parallelize(edgeArray)
      val verticesRDD : RDD[(VertexId, (String))]= sc.parallelize(verticesArray)
      val graph = Graph(verticesRDD, edgeRDD)
      val initialGraph = graph.mapVertices((id,_)=> if(id.toString == sourceNodeID.toString) 0.0 else Double.PositiveInfinity)
      val sssp = initialGraph.pregel(Double.PositiveInfinity)(
          (id, dist,newDist) => math.min(dist, newDist),
          triplet => {
            if(triplet.srcAttr + triplet.attr < triplet.dstAttr){
              Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
            }else{
              Iterator.empty
            }
          },
          (a,b) => math.min(a,b)
      );
      println(sssp.vertices.filter(x => x._1.toLong != sourceNodeID.toLong && x._2 != Double.PositiveInfinity).collect().length)
    }
}