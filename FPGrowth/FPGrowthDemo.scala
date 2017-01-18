package LocalDemo

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhu on 2017/1/16.
  */
object FPGrowthDemo {
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("FPGrowthDemo").setMaster("local")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val minSupport = args(1).toDouble    //最小支持度
    val minConfidence = args(2).toDouble//最小置信度
    val numPartition = args(3).toInt    //数据分区

    val data = sc.textFile(dataPath)

    val transaction : RDD[Array[String]] = data.map(s => s.split(" ")).cache()

    val fPGrowth = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition) //创建一个FPGrowth算法的实例

    val model = fPGrowth.run(transaction)//调用算法

//    查看所有频繁项集并列出它出现的次数
    model.freqItemsets.collect().foreach(itemset =>{
      println(itemset.items.mkString("[",",","]") + "," + itemset.freq)
    })

    //通过置信度筛选出推荐规则
    // antecedent表示前项
    // consequence表示后项
    //confidence表示规则的置信度
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
    //查看规则生成的数量
    println(model.generateAssociationRules(minConfidence).collect().length)
  }
}
