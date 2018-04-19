package com.uk.odinconsultants.linalg

import org.apache.spark.ml.linalg.{SparseVector, Vector => SVector}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DimensionCompression {

  def nonZeroIndicesOf(x: SparseVector): Set[Int] = x.indices.toSet

  def extractVec(colIndx: Int)(r: Row): SparseVector = r.getAs[SparseVector](colIndx)

  def ids(colIndx: Int)(r1: Row, r2: Row): Row = {
    println(s"Rows:\n$r1\n$r2\nRow 1 length = ${r1.length}")
    val extractor = extractVec(colIndx) _
    val vec1    = extractor(r1)
    val vec2    = extractor(r2)
    Row(vec1.indices.toSet ++ vec2.indices.toSet)
  }

  def indicesIn(df: DataFrame, colIndx: Int): Set[Int] = {
    import df.sparkSession.implicits._
    val extractor = extractVec(colIndx) _
    val indices = df.flatMap { r => extractor(r).indices }.distinct()
    indices.collect().toSet
  }

  def discardEmptyColumns(df: DataFrame, colIndx: Int): DataFrame = {
    val fn: (Row, Row) => Row = ids(colIndx)
    val reduced = df.reduce(fn)
    ???
  }

}
