package com.uk.odinconsultants.palladian

object Text {

  def alphaNumeric(x: String): String = x.replaceAll("""[^a-zA-Z0-9]""", "")

}
