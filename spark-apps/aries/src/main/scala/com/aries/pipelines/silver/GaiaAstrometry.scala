import com.aries.core.Pipeline
import com.aries.common.iceberg.IcebergManager
import cats.effect.IO
import java.sql.Connection
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

// object GaiaAstrometry extends Pipeline {
// }
