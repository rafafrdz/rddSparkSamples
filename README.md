# EJEMPLOS RDDS EN SPARK

Se desea crear ciertos rdds a partir de un csv guardado en nuestro sistema de ficheros que consista en una colección de tipo case class definida en la libreria Lib.scala

* CrearRddLA.scala

```scala
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
```

* CrearRddSFP

```scala
import Lib
import CrearRddLA.fechas
import org.apache.spark.{SparkConf, SparkContext}

object CrearRddSF {
  // crear un rdd usando el case class CallsServ iceSFPD y lo filtramos por el campo fecha cambiando
  // el formato a dd/mm/aaaa por los años 2014 2015 y lo guardamos como archivo de txt en hdfs
  def main(args: Array[String]): Unit = {
    val sfpd = Lib.getDatasetSFPD("/opt/datasets/csv/sf_pd_incident_reports.csv")
    val conf = new SparkConf().setMaster("local[*]").setAppName("CrearRddSF")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd = sc.parallelize(sfpd).flatMap(m=>m match {
      case Lib.CallsServiceSFPD(a,b,c,d,fecha,f,g,h,i,j,k,l,m) if(fecha.contains("2014") || fecha.contains("2015")) => Some(Lib.CallsServiceSFPD(a,b,c,d,fechas(fecha),f,g,h,i,j,k,l,m))
      case _ => None
    })
    //rdd.foreach(println)
    // Conexión con el hdfs
    rdd.saveAsTextFile("hdfs://192.168.8.100:7232/datasets/txt/fechas.txt")
  }
}
```

* CrearRddSFP con mapPartition

```scala
import Lib
import CrearRddLA.fechas
import org.apache.spark.{SparkConf, SparkContext}

// Analogo pero usando mapPartitions
object CrearRddSFPartition {
  def main(args: Array[String]): Unit = {
    val sfpd = Lib.getDatasetSFPD("/opt/datasets/csv/sf_pd_incident_reports.csv")
    val conf = new SparkConf().setMaster("local[*]").setAppName("CrearRddSF")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd = sc.parallelize(sfpd).mapPartitions(_.flatMap(m=>m match {
      case Lib.CallsServiceSFPD(a,b,c,d,fecha,f,g,h,i,j,k,l,m) if(fecha.contains("2014") || fecha.contains("2015")) => Some(Lib.CallsServiceSFPD(a,b,c,d,fechas(fecha),f,g,h,i,j,k,l,m))
      case _ => None
    }))
    //rdd.foreach(println)
    rdd.saveAsTextFile("hdfs://192.168.8.100:7232/datasets/txt/fechas.txt")
  }
}
```

