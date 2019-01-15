import m2c.spark.base.Lib
import m2c.spark.rdd.CrearRddLA.fechas
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
    rdd.foreach(println)
    //rdd.saveAsTextFile("hdfs://192.168.8.100:7232/datasets/txt/fechas.txt")
  }
}
