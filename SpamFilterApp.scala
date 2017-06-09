// spamFilter.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object spamFilter {
    
  // 3
  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {

    // a. Prend un String renvoie -> RDD[(String,String)]
    val files = sc.wholeTextFiles(filesDir + "/*.txt")

    // b. Prend un RDD[(String,String)] -> Int
    val nbFiles = files.map(x => 1).reduce(_+_)

    // c. Prend un RDD[(String,String)] -> RDD[String]
    var listWords = files.flatMap(x => x._2.split("\\s+").distinct)

    // d. Prend un RDD[String] et un Set[String] -> RDD[String]
    val nonWords = Set[String](".",":",",","/","\\","(",")","@","-","\'"," ")
    val filteredListWords = listWords.filter(!nonWords(_)) 

    // e. Prend un RDD[String] -> RDD[(String,Int)]
    val wordDirOccurency = filteredListWords.groupBy(identity).mapValues(_.size)

    // f. Prend un RDD[String,Int] -> RDD[(String,Double)]
    val probaWord = wordDirOccurency.mapValues(_ / nbFiles.toDouble)

    // return RDD[(String,Double)] et un Long
    (probaWord, nbFiles)
  }


  // 4
  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],
    probaW:RDD[(String, Double)],
    probaC: Double,
    probaDefault: Double // default value when a probability is missing
  ):RDD[(String, Double)] = {
  			// Prend 2 RDD[(String,Double)]
    val res =probaW.leftOuterJoin(probaWC).mapValues({
        case (x, Some(y)) => y * (math.log(y / (x * probaC))/math.log(2))
        case (x, None) => probaDefault * (math.log(probaDefault / (x * probaC))/math.log(2))
    })
    // return un RDD[(String,Double)]
    (res)
  }




  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spam Filter App")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR") // Permet de reduire le verbose des logs
    
    // 5 
    // a. probaWordDir prend en argument un context et un String -> (RDD[(String,Double)],Long)
    val (probaWordSpam, nbFilesSpam) = probaWordDir(sc)("/tmp/ling-spam/spam")
    val (probaWordHam, nbFilesHam) = probaWordDir(sc)("/tmp/ling-spam/ham")

    // b. Calcule de probaDefault et des probabilités de Spam et Ham en fonction du nombre de fichiers totales.
    val probaDefault = 0.2/(nbFilesSpam + nbFilesHam).toDouble
    val probaSpam = nbFilesSpam/(nbFilesSpam + nbFilesHam).toDouble
    val probaHam = nbFilesHam/(nbFilesSpam + nbFilesHam).toDouble
    
    // Prend un RDD[(String,Double)] et un Double  -> RDD[(String,Double)]
    val probaWS = probaWordSpam.mapValues( _ * probaSpam)
    val probaWH = probaWordHam.mapValues( _ * probaHam)

    //Prend un RDD[(String,Double)] et un Double -> RDD[(String,Double)]
    val inverseProbaS = probaWordSpam.mapValues(1-_).mapValues( _ * probaSpam)
    val inverseProbaH = probaWordHam.mapValues(1-_).mapValues( _ * probaHam)
    
    // Prend en 2 RDD[(String,Double)] -> RDD[(String,Double)]
    val probaW = probaWH.fullOuterJoin(probaWS).mapValues({
        case (Some(probaWH), Some( probaWS)) => probaWS + probaWH
        case (Some(probaWH), None) => probaDefault + probaWH
        case (None, Some(probaWS)) => probaDefault + probaWS
    })

    // Prend un RDD[(String,Double)] -> RDD[(String,Double)]
    val inverseProbaWords = probaW.mapValues(1-_)
    val inverseProbaW = inverseProbaWords.mapValues({
        case(0) => probaDefault
        case(x) => x
    })

    // c. Prend en arguments 2 RDD[(String,Double)] et 2 Double -> RDD[(String,Double)]
    val resS = computeMutualInformationFactor(probaWS, probaW, probaSpam, probaDefault)
    val resH = computeMutualInformationFactor(probaWH, probaW, probaHam, probaDefault)
    val resSF = computeMutualInformationFactor(inverseProbaS, inverseProbaW, probaSpam, probaDefault)
    val resHF = computeMutualInformationFactor(inverseProbaH, inverseProbaW, probaHam, probaDefault)

    // Prend 2 RDD[(String,Double)] -> RDD[(String,Double)]
    val res1 = resS.join(resH).mapValues({case (resS,resH) => resS + resH})
    val res2 = resSF.join(resHF).mapValues({case (resSF,resHF) => resSF + resHF})
    val mutualInformation = res1.join(res2).mapValues({case (res1,res2) => res1 + res2})
    
    // d. Prend un RDD[(String,Double)] -> Array[String]
    val wordsMI = mutualInformation.takeOrdered(10)(Ordering.by { - _._2 }).map(x => x._1)
    wordsMI.foreach(println)


    // e. Ecriture du fichier dans HDFS
    val conf1 = new Configuration()
    conf1.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020")
    val fs= FileSystem.get(conf1)
    val os = fs.create(new Path("/tmp/topWords.txt"))
    try {
    	// Transforme le Array[String] -> String
        var str = wordsMI.mkString("\n")
        os.write(str.getBytes)
    }
    finally {
        os.close()
    }
  }


} // end of spamFilter
