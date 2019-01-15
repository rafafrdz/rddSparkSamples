import Lib // Lib.scala
import org.apache.spark.{SparkConf, SparkContext} // Libreria externa de Spark

object CrearRddLA {

  def main(args: Array[String]): Unit = {

    val lapd = Lib.getDatasetLAPD(2015).take(10) //Demasiado recurso de memoria, cogemos 10
    val lapd2 = Lib.getDatasetLAPD(2016).take(10)
    val lapd3 = Lib.getDatasetLAPD(2017).take(10)

    // Spark
    val conf = new SparkConf().setMaster("local[*]").setAppName("RddsAnnos")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val (rdd1, rdd2, rdd3) = (
      sc.parallelize(lapd).map(m=> m match{
        case Lib.CallsServiceLAPD(a,b,c,fecha,d,e,f) => Lib.CallsServiceLAPD(a,b,c,fechas(fecha),d,e,f)
      }),
      sc.parallelize(lapd2).map(m=> m match{
        case Lib.CallsServiceLAPD(a,b,c,fecha,d,e,f) => Lib.CallsServiceLAPD(a,b,c,fechas(fecha),d,e,f)
      }),
      sc.parallelize(lapd3).map(m=> m match{
        case Lib.CallsServiceLAPD(a,b,c,fecha,d,e,f) => Lib.CallsServiceLAPD(a,b,c,fechas(fecha),d,e,f)
      }))
    val rddUnion = sc.union(rdd1,rdd2,rdd3).groupBy(_.distrito).flatMap(_._2)
    // Conexión con el hdfs
    rddUnion.saveAsTextFile("hdfs://192.168.8.100:7232/datasets/txt/distritos.txt")


  }
  def fechas(fecha:String):String={
    val fechita = fecha.split("/")
    val (mes, dia, anno) = (fechita(0),fechita(1),fechita(2))
    s"$dia/$mes/$anno"
  }

}
