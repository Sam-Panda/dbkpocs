import com.macasaet.fernet.{Key, StringValidator, Token}
import java.time.{Duration, Instant}
import org.apache.hadoop.hive.ql.exec.UDF;

class Validator extends StringValidator {

  override def getTimeToLive() : java.time.temporal.TemporalAmount = {
    Duration.ofSeconds(Instant.MAX.getEpochSecond());
  }
}

class udfDecrypt extends UDF {

  def evaluate(inputVal: String, sparkKey : String): String = {

    if( inputVal != null && inputVal!="" ) {
      val keys: Key = new Key(sparkKey)
      val token = Token.fromString(inputVal)
      val validator = new Validator() {}
      val payload = token.validateAndDecrypt(keys, validator)
      payload
    } else return inputVal
  }
}