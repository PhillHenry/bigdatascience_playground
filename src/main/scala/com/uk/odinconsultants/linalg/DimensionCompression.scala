package com.uk.odinconsultants.linalg

import org.apache.spark.ml.linalg.{SparseVector, Vector => SVector}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.{DataFrame, Row}

object DimensionCompression {

  def toIndices(i: Int)(r: Row): Array[Int] = r.getAs[SparseVector](0).indices

  def compacting(replacements: Map[Int, Int])(v: SparseVector): SparseVector = {
    val indexVals  = v.indices.zip(v.values)
    val newIndices = indexVals.map { case (i, x) => (replacements(i), x) }.sortBy(_._1)
    new SparseVector(replacements.size, newIndices.map(_._1), newIndices.map(_._2))
  }

  def compactingUdf(replacements: Map[Int, Int]): UserDefinedFunction = {
    val compactingFn = compacting(replacements) _
    udf(compactingFn)
  }

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
    val allIndices    = indicesIn(df, colIndx)
    val old2New       = allIndices.zipWithIndex.toMap
    val colName       = df.columns.apply(colIndx)
    val fn            = compactingUdf(old2New)
    df.withColumn(colName, fn(col(colName)))
  }

}
