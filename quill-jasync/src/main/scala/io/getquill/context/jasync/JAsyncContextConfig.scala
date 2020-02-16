package io.getquill.context.jasync

import java.nio.charset.Charset

import com.github.jasync.sql.db.ConcreteConnection
import com.github.jasync.sql.db.{ ConnectionPoolConfiguration, ConnectionPoolConfigurationBuilder }
import com.github.jasync.sql.db.pool.ConnectionPool
import scala.util.Success
import com.github.jasync.sql.db.Configuration
import com.github.jasync.sql.db.SSLConfiguration
import com.github.jasync.sql.db.pool.ObjectFactory
import com.github.jasync.sql.db.util.AbstractURIParser
import scala.collection.JavaConversions._
import com.typesafe.config.Config

import scala.util.Try

abstract class JAsyncContextConfig[C <: ConcreteConnection](
  config:            Config,
  connectionFactory: Configuration => ObjectFactory[C],
  uriParser:         AbstractURIParser
) {

  private def getValue[T](path: String, getter: String => T) = Try(getter(path))
  private def getString(path: String) = getValue(path, config.getString)
  private def getInt(path: String) = getValue(path, config.getInt)
  private def getLong(path: String) = getValue(path, config.getLong)

  private lazy val default = new ConnectionPoolConfigurationBuilder().build()

  lazy val connectionPoolConfiguration = new ConnectionPoolConfiguration(
    getString("host").getOrElse(default.getHost),
    getInt("port").getOrElse(default.getPort),
    getString("database").toOption.orElse(Option(default.getDatabase)).orNull,
    getString("username").getOrElse(default.getUsername),
    getString("password").toOption.orElse(Option(default.getPassword)).orNull,
    getInt("maxActiveConnections").getOrElse(default.getMaxActiveConnections),
    getLong("maxIdleTime").getOrElse(default.getMaxIdleTime),
    getInt("maxPendingQueries").getOrElse(default.getMaxPendingQueries),
    getLong("connectionValidationInterval").getOrElse(default.getConnectionValidationInterval),
    getLong("connectionCreateTimeout").getOrElse(default.getConnectionCreateTimeout),
    getLong("connectionTestTimeout").getOrElse(default.getConnectionTestTimeout),
    getLong("queryTimeout").toOption.map(new java.lang.Long(_)).orElse(Option(default.getQueryTimeout)).orNull,
    default.getEventLoopGroup,
    default.getExecutionContext,
    default.getCoroutineDispatcher,
    new SSLConfiguration(
      Map(
        "sslmode" -> getString("sslmode"),
        "sslrootcert" -> getString("sslrootcert")
      ).collect {
          case (key, Success(value)) => key -> value
        }
    ),
    Try(Charset.forName(config.getString("charset"))).getOrElse(default.getCharset),
    getInt("maximumMessageSize").getOrElse(default.getMaximumMessageSize),
    default.getAllocator,
    getString("applicationName").toOption.orElse(Option(default.getApplicationName)).orNull,
    default.getInterceptors,
    getLong("maxConnectionTtl").toOption.map(new java.lang.Long(_)).orElse(Option(default.getMaxConnectionTtl)).orNull
  )

  lazy val configuration = connectionPoolConfiguration.getConnectionConfiguration

  lazy val poolConfiguration = connectionPoolConfiguration.getPoolConfiguration

  def pool =
    new ConnectionPool[C](
      connectionFactory(configuration),
      connectionPoolConfiguration
    )
}
