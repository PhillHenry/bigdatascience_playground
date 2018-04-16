package com.uk.odinconsultants.palladian

import org.scalatest.{Matchers, WordSpec}

class TextSpec extends WordSpec with Matchers {

  import Text._

  "Text" should {
    "be alpha numeric only" in {
      val actual = "12phill"
      alphaNumeric(s"!%^${actual}*()") shouldBe actual
    }
  }

}
