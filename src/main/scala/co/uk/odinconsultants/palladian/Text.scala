package co.uk.odinconsultants.palladian

object Text {

  def alphaNumeric(x: String): String = x.replaceAll("""[^a-zA-Z0-9]""", "")

  def toNgrams(x: String, min: Int, max: Int): Seq[String] = (min to max).flatMap(i => x.sliding(i))

}
