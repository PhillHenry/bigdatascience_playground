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
    val indices = (1 to n).map { _ => rnd.nextInt(vecSize) }.toSet[Int].toArray.sorted
    val values  = (1 to indices.length).map { _ => rnd.nextDouble() }.toArray
    val vec: SparseVector = new SparseVector(vecSize, indices, values)
    (i, vec)
  }

  val colName               = "VECTORS"
  val COL_INDEX             = 1
  val df: DataFrame         = vectors.toDF("ID", colName)
  val allIndices: Set[Int]  = vectors.flatMap(_._2.indices).toSet

  "non empty columns" should {
    "be recorded" in {
      indicesIn(df, COL_INDEX) shouldBe allIndices
    }
  }

  "empty columns" should {
    "be removed" in {
      withClue("Not much of a test if this is not true") {
        allIndices.size should be < (vecSize)
      }

      val actual  = discardEmptyColumns(df, COL_INDEX)
      val inMem   = actual.collect()
      inMem.foreach { r =>
        val v = extractVec(COL_INDEX)(r)
        v.size shouldBe allIndices.size
        v.indices.foreach { i => i shouldBe < (allIndices.size) }
      }
    }
  }

}
