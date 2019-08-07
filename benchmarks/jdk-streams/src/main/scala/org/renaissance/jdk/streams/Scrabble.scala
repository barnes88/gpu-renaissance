package org.renaissance.jdk.streams

import org.renaissance.Benchmark
import org.renaissance.Benchmark._
import org.renaissance.BenchmarkContext
import org.renaissance.BenchmarkResult
import org.renaissance.License

import scala.collection.JavaConverters

@Name("scrabble")
@Group("jdk-streams")
@Summary("Solves the Scrabble puzzle using JDK Streams.")
@Licenses(Array(License.GPL2))
@Repetitions(50)
@Parameter(name = "input_path", defaultValue = "/shakespeare.txt")
@Parameter(
  name = "expected_result",
  defaultValue = "120--QUICKLY,118--ZEPHYRS,114--QUALIFY-QUICKEN-QUICKER"
)
@Configuration(
  name = "test",
  settings = Array(
    "input_path = /shakespeare-truncated.txt",
    "expected_result = 120--QUICKLY,114--QUICKEN-QUICKER,108--BLAZING-PRIZING"
  )
)
@Configuration(name = "jmh")
final class Scrabble extends Benchmark {

  // TODO: Consolidate benchmark parameters across the suite.
  //  See: https://github.com/renaissance-benchmarks/renaissance/issues/27

  private var inputPathParam: String = _

  private var expectedResultParam: Seq[String] = _

  private val scrabblePath = "/scrabble.txt"

  private var scrabble: JavaScrabble = _

  override def setUpBeforeAll(c: BenchmarkContext): Unit = {
    inputPathParam = c.stringParameter("input_path")
    expectedResultParam = c.stringParameter("expected_result").split(",").map(_.trim).toSeq
    scrabble = new JavaScrabble(inputPathParam, scrabblePath)
  }

  override def run(c: BenchmarkContext): BenchmarkResult = {
    val result = scrabble.run()

    () => {
      val actualWords = JavaScrabble.prepareForValidation(result)
      BenchmarkResult.assertEquals(
        expectedResultParam.size,
        actualWords.size,
        "best words count"
      )

      for ((expected, actual) <- expectedResultParam zip JavaConverters.asScalaBuffer(
             actualWords
           )) {
        BenchmarkResult.assertEquals(expected, actual, "best words")
      }
    }
  }
}
