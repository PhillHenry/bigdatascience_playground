package co.uk.odinconsultants.spark.feature

import org.scalatest.{Matchers, WordSpec}

class NAndSmallerGramSpec extends WordSpec with Matchers {

  "smaller n-grams" should {
    "be generated along with larger ones" in {
      val n       = 2
      val nf      = new NAndSmallerGram().setN(n).createTransformFunc
      val nWords  = 5
      val text    = (1 to nWords).map(_.toString)
      val ngrams  = nf(text)
      ngrams should have size (nWords + (nWords - 1))
    }
  }

}
