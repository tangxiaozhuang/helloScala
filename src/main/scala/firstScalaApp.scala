import org.apache.spark.{SparkConf, SparkContext}

object firstScalaApp {
  def main(args: Array[String]): Unit = {
    //test.txt文件里包含了hello world行，读者可自己修改测试
    val inputfile="hdfs://hadoop1:9000/test.txt"

    val conf = new SparkConf().setAppName("HelloWorld").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val textfile = sc.textFile(inputfile)

    //查询包含hello world的行
    val lines = textfile.filter(line => line.contains("hello world"))

    lines.foreach(println)
  }
}
