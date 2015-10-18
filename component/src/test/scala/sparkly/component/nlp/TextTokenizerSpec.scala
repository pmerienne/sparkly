package sparkly.component.nlp

import org.scalatest._
import java.nio.file.{Paths, Files}
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.io.File
import java.util.UUID

class TextTokenizerSpec extends FlatSpec with Matchers {

  "TextTokenizer" should "tokenize english sentence" in {
    // Given
    val tokenizer = TextTokenizer(language = "English")
    val text = "I can't argue with some arguments on argus with argues"

    // When
    val tokens = tokenizer.tokenize(text)

    // Then
    tokens should be (List("i", "can't", "argu", "some", "argument", "argu", "argu"))
  }

  "TextTokenizer" should "tokenize french sentence" in {
    // Given
    val tokenizer = TextTokenizer(language = "French")
    val text = "Mes amis vont bientot arriver."

    // When
    val tokens = tokenizer.tokenize(text)

    // Then
    tokens should be (List("ami", "vont", "bientot", "ariv"))
  }

  "TextTokenizer" should "tokenize without language" in {
    // Given
    val tokenizer = TextTokenizer()
    val text = "I can't argue with some arguments on argus with argues"

    // When
    val tokens = tokenizer.tokenize(text)

    // Then
    tokens should be (List("i", "can't", "argue", "with", "some", "arguments", "on", "argus", "with", "argues"))
  }

  "TextTokenizer" should "tokenize english sentence with ngram" in {
    // Given
    val tokenizer = TextTokenizer(language = "English", minNGram = 2, maxNGram = 3)
    val text = "Trying ngram filtering is really hard"

    // When
    val tokens = tokenizer.tokenize(text)

    // Then
    tokens should be (List("try", "try ngram", "try ngram filter", "ngram", "ngram filter", "filter", "realli", "realli hard", "hard"))
  }

  "TextTokenizer" should "remove patterns" in {
    // Given
    val tokenizer = TextTokenizer(language = "English", ignorePatterns = List("[1-9](\\w+)*", "#(\\w+)*", "&amp;"))
    val text = "I bought 3 Apples &amp; 4 oranges 4U #food"

    // When
    val tokens = tokenizer.tokenize(text)

    // Then
    tokens should be (List("i", "bought", "appl", "orang"))
  }

  "TextTokenizer" should "replace patterns" in {
    // Given
    val tokenizer = TextTokenizer(language = "English", replacePatterns = Map("([a-z])\\1{1,}" -> "$1"))
    val text = "wazuuup"

    // When
    val tokens = tokenizer.tokenize(text)

    // Then
    tokens should be (List("wazup"))
  }

}
