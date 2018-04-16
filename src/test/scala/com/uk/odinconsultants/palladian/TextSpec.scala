package com.uk.odinconsultants.palladian

import org.scalatest.{Matchers, WordSpec}

import scala.annotation.tailrec

class TextSpec extends WordSpec with Matchers {

  import Text._

  @tailrec
  final def factorial(x: Int, acc: Int): Int = {
    if (x <= 1) {
      acc
    } else {
      factorial(x - 1, acc * x)
    }
  }

  "Text" should {
    "be alpha numeric only" in {
      val actual = "12phill"
      alphaNumeric(s"!%^${actual}*()") shouldBe actual
    }
    "turned into n-grams" in {
      val text    = "0123456789"
      val length  = text.length
      val max     = 10
      val min     = 3
      val ngrams  = toNgrams(text, min, max)
      ngrams should have length (min to max).map(x => max - x + 1).sum
    }
  }


}
