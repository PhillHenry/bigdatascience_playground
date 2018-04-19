package com.uk.odinconsultants.linalg

import org.apache.spark.ml.linalg.{SparseVector, Vector => SVector}
import org.apache.spark.sql.DataFrame
import org.scalatest.{Matchers, WordSpec}
import com.uk.odinconsultants.spark.SparkForTesting._
import scala.collection.immutable.Seq

import scala.util.Random

class DimensionCompressionIntegrationSpec extends WordSpec with Matchers {

  import DimensionCompression._
  import sqlContext.implicits._

  val nVectors  = 100
  val vecSize   = 1000
  val vectors: Seq[(Int, SparseVector)] = (1 to nVectors).map { i =>
    val n       = 3
    val rnd     = new Random()
    val indices = (1 to n).map { _ => rnd.nextInt(vecSize) }.toSet.toArray.sorted
    val values  = (1 to indices.length).map { _ => rnd.nextDouble() }.toArray
    val vec: SparseVector = new SparseVector(vecSize, indices, values)
    (i, vec)
  }

  val colName       = "VECTORS"
  val df: DataFrame = vectors.toDF("ID", colName)
  val allIndices    = vectors.flatMap(_._2.indices).toSet

  "non empty columns" should {
    "be recorded" in {
      indicesIn(df, 1) shouldBe allIndices
    }
  }

  "empty columns" should {
    "be removed" ignore {
      df.show()

      discardEmptyColumns(df, 1)
    }
  }

}
