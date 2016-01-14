package test

import org.apache.spark.SparkConf
import java.net.URL
import java.net.URLConnection
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import java.util.Arrays
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import java.util.List
import java.util.Map
import scala.collection.JavaConversions._
import javax.naming.ldap.LdapName
import javax.naming.ldap.Rdn
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLSession
import org.apache.spark.streaming.{ Seconds }
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import java.io._
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import com.datastax.spark.connector.SomeColumns
import scala.Serializable

object StreamDirectoryDataVer1 {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: StreamDirectoryDataVer1 <duration> <dataDirectory> <outputDirectory>")
      System.exit(1)
    }
    
    //Setting the required inputs
    val duration = args(0);
    val inputDirectory = args(1)
    val outputDirectory = args(2)
    val destIpFieldIndicator = args(3).toInt

    // Create the context with a 1 second batch size
    //192.168.91.10sslCache
    val sparkConf = new SparkConf().setAppName("StreamDirectoryDataVer1").set("spark.cassandra.connection.host", "192.168.91.7").set("spark.driver.allowMultipleContexts","true")
    val ssc = new StreamingContext(sparkConf, Seconds(duration.toLong))
    //val sc = new SparkContext(sparkConf);
    //sc.textFile(", minPartitions)
    val directoryData = ssc.textFileStream(inputDirectory);
    //directoryData.saveAsTextFiles(outputDirectory + "_dirData_")
    val lines = directoryData.map {s => (s,0)}
    val distinctIps = lines.groupByKey()
    val  sslcache = distinctIps.map{case(x,y) => (x,getDomainNameBySSLCertLookup(x.toString()))};                                                        
    sslcache.print();
    sslcache.saveAsTextFiles(outputDirectory + "_vanna_");
   // sslcache.foreachRDD(rdd => { rdd.saveAsTextFile(outputDirectory + "_vanna_")
    //})
    ssc.start();
    ssc.awaitTermination();
}
  
  //SSL Cert LookUp 
   def getDomainNameBySSLCertLookup(destIp: String): String = {
     
     /*var cassTable = ssc.cassandraTable("dev", "sslcache");
     if(!cassTable.select("ip", "domain_name").where("ip = ?",destIp).isEmpty()) 
     {*/
     var domainName: String = null;
      try {
      val url = new URL("https://" + destIp)
      var conn = url.openConnection().asInstanceOf[HttpsURLConnection]
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)
      conn.setHostnameVerifier(new HostnameVerifier() {

        def verify(arg0: String, arg1: SSLSession): Boolean = return true
      })
      try {
        conn.connect()
      } catch {
        case ex: Exception => conn = null
      }
      if (conn != null) {
        val certs = conn.getServerCertificates
        val xcert = certs(0).asInstanceOf[X509Certificate]
        val dn = xcert.getSubjectX500Principal.getName
        val ldapDN = new LdapName(dn)
        for (rdn <- ldapDN.getRdns) {
          if (rdn.getType == "CN") {
            domainName = rdn.getValue.asInstanceOf[String]
            if (domainName.startsWith("*.")) {
              domainName = domainName.substring(domainName.indexOf("*.") + 2)
            }
          } else if (rdn.getType == "O") {
            val organization = rdn.getValue.asInstanceOf[String]
          }
        }
        conn.disconnect()
      }
    } catch {
      case e: Exception => println("Exception")
    }
     domainName
  }
   //HTTP header lookup.
   def getDomainNameByHttpHeaderLookup(destIp: String): String = {
    var domainName: String = null
    try {
      val url = new URL("http://" + destIp + ":80")
      val conn = url.openConnection()
      conn.setConnectTimeout(java.lang.Integer.parseInt("2000"))
      conn.setReadTimeout(java.lang.Integer.parseInt("2000"))
      val headerFields = conn.getHeaderFields
      for (headerFieldKey <- headerFields.keySet if "Set-Cookie".equalsIgnoreCase(headerFieldKey)) {
        val headerFieldValue = headerFields.get(headerFieldKey)
        for (headerValue <- headerFieldValue) {
          val fields = headerValue.split(";\\s*")
          for (j <- 1 until fields.length if fields(j).indexOf('=') > 0) {
            val f = fields(j).split("=")
            if ("domain".equalsIgnoreCase(f(0))) {
              domainName = f(1)
              if (domainName != null) {
                return domainName
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => println("Some Eror occured")
    }
    domainName
  }
}