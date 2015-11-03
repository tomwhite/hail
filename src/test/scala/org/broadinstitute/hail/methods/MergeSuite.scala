import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test

class MergeSuite extends SparkSuite {
  @Test def test() = {
    val arr = Array(Array(15,16,17,18,19,20,21,18), Array(25,25,26,28,15,30,15,20),
      Array(35,36,37,45,32,44,20,33), Array(20,21,13,23,25,27,16,22))

    val rdd1 = TestRDDBuilder.buildRDD(5, 2, sc, "sparky", dpArray = Some(arr), gqArray = None)
    val rdd2 = TestRDDBuilder.buildRDD(5, 2, sc, "sparky", dpArray = Some(arr), gqArray = None)
    println(rdd1.nSamples)
    println(rdd1.nVariants)
    println(rdd2.nSamples)
    println(rdd2.nVariants)
  }
}