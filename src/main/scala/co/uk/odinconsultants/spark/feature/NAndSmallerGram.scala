package co.uk.odinconsultants.spark.feature

import org.apache.spark.ml.feature.NGram

class NAndSmallerGram extends NGram {

  override def createTransformFunc: Seq[String] => Seq[String] = { text =>
    (1 to $(n)).flatMap { i =>
      text.iterator.sliding(i).withPartial(false).map(_.mkString(" ")).toSeq
    }
  }

}
