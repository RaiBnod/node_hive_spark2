const fs = require('fs');

var HttpStatus = require('http-status-codes');
var JDBC = require('jdbc');
var jinst = require('jdbc/lib/jinst');

if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath([
    './drivers/apache-hadoop/hadoop-core-1.2.1.jar',
    './drivers/spark-hive/joda-time-2.5.jar',
    './drivers/spark-hive/json-20090211.jar',
    './drivers/spark-hive/protobuf-java-2.5.0.jar',
    './drivers/spark-hive/antlr-2.7.7.jar',
    './drivers/spark-hive/datanucleus-core-3.2.10.jar',
    './drivers/spark-hive/commons-codec-1.4.jar',
    './drivers/spark-hive/geronimo-jta_1.1_spec-1.1.1.jar',
    './drivers/spark-hive/jersey-json-1.9.jar',
    './drivers/spark-hive/jta-1.1.jar',
    './drivers/spark-hive/commons-httpclient-3.1.jar',
    './drivers/spark-hive/jetty-6.1.26.jar',
    './drivers/spark-hive/jersey-server-1.9.jar',
    './drivers/spark-hive/jettison-1.1.jar',
    './drivers/spark-hive/aopalliance-1.0.jar',
    './drivers/spark-hive/curator-client-2.6.0.jar',
    './drivers/spark-hive/opencsv-2.3.jar',
    './drivers/spark-hive/libfb303-0.9.2.jar',
    './drivers/spark-hive/apache-log4j-extras-1.2.17.jar',
    './drivers/spark-hive/mail-1.4.1.jar',
    './drivers/spark-hive/curator-framework-2.6.0.jar',
    './drivers/spark-hive/leveldbjni-all-1.8.jar',
    './drivers/spark-hive/asm-tree-3.1.jar',
    './drivers/spark-hive/servlet-api-2.5.jar',
    './drivers/spark-hive/commons-dbcp-1.4.jar',
    './drivers/spark-hive/hive-service-1.2.1.spark2.jar',
    './drivers/spark-hive/hive-shims-0.20S-1.2.1.spark2.jar',
    './drivers/spark-hive/commons-collections-3.2.1.jar',
    './drivers/spark-hive/hadoop-yarn-server-common-2.6.0.jar',
    './drivers/spark-hive/jaxb-impl-2.2.3-1.jar',
    './drivers/spark-hive/antlr-runtime-3.4.jar',
    './drivers/spark-hive/log4j-1.2.16.jar',
    './drivers/spark-hive/hadoop-yarn-server-applicationhistoryservice-2.6.0.jar',
    './drivers/spark-hive/parquet-hadoop-bundle-1.6.0.jar',
    './drivers/spark-hive/hadoop-yarn-server-web-proxy-2.6.0.jar',
    './drivers/spark-hive/javax.inject-1.jar',
    './drivers/spark-hive/datanucleus-rdbms-3.2.9.jar',
    './drivers/spark-hive/activation-1.1.jar',
    './drivers/spark-hive/hive-metastore-1.2.1.spark2.jar',
    './drivers/spark-hive/geronimo-jaspic_1.0_spec-1.0.jar',
    './drivers/spark-hive/jackson-core-asl-1.9.13.jar',
    './drivers/spark-hive/jetty-all-7.6.0.v20120127.jar',
    './drivers/spark-hive/hive-common-1.2.1.spark2.jar',
    './drivers/spark-hive/hadoop-yarn-api-2.6.0.jar',
    './drivers/spark-hive/jackson-xc-1.8.3.jar',
    './drivers/spark-hive/paranamer-2.3.jar',
    './drivers/spark-hive/jersey-client-1.9.jar',
    './drivers/spark-hive/hive-shims-1.2.1.spark2.jar',
    './drivers/spark-hive/slf4j-api-1.7.5.jar',
    './drivers/spark-hive/stax-api-1.0-2.jar',
    './drivers/spark-hive/commons-cli-1.2.jar',
    './drivers/spark-hive/slf4j-log4j12-1.7.5.jar',
    './drivers/spark-hive/commons-io-2.4.jar',
    './drivers/spark-hive/guice-servlet-3.0.jar',
    './drivers/spark-hive/httpclient-4.4.jar',
    './drivers/spark-hive/zookeeper-3.4.6.jar',
    './drivers/spark-hive/hive-serde-1.2.1.spark2.jar',
    './drivers/spark-hive/libthrift-0.9.2.jar',
    './drivers/spark-hive/hive-shims-scheduler-1.2.1.spark2.jar',
    './drivers/spark-hive/guava-14.0.1.jar',
    './drivers/spark-hive/jdo-api-3.0.1.jar',
    './drivers/spark-hive/commons-compress-1.4.1.jar',
    './drivers/spark-hive/datanucleus-api-jdo-3.2.6.jar',
    './drivers/spark-hive/derby-10.10.2.0.jar',
    './drivers/spark-hive/hive-shims-0.23-1.2.1.spark2.jar',
    './drivers/spark-hive/asm-3.1.jar',
    './drivers/spark-hive/jackson-mapper-asl-1.9.13.jar',
    './drivers/spark-hive/guice-3.0.jar',
    './drivers/spark-hive/commons-logging-1.1.3.jar',
    './drivers/spark-hive/avro-1.7.5.jar',
    './drivers/spark-hive/geronimo-annotation_1.0_spec-1.1.1.jar',
    './drivers/spark-hive/hadoop-yarn-server-resourcemanager-2.6.0.jar',
    './drivers/spark-hive/ant-launcher-1.9.1.jar',
    './drivers/spark-hive/hive-jdbc-1.2.1.spark2.jar',
    './drivers/spark-hive/jdk.tools-1.7.jar',
    './drivers/spark-hive/jpam-1.1.jar',
    './drivers/spark-hive/snappy-java-1.0.5.jar',
    './drivers/spark-hive/ant-1.9.1.jar',
    './drivers/spark-hive/commons-lang-2.6.jar',
    './drivers/spark-hive/stringtemplate-3.2.1.jar',
    './drivers/spark-hive/httpcore-4.4.jar',
    './drivers/spark-hive/jersey-guice-1.9.jar',
    './drivers/spark-hive/asm-commons-3.1.jar',
    './drivers/spark-hive/jetty-util-6.1.26.jar',
    './drivers/spark-hive/jaxb-api-2.2.2.jar',
    './drivers/spark-hive/netty-3.7.0.Final.jar',
    './drivers/spark-hive/jsr305-3.0.0.jar',
    './drivers/spark-hive/xz-1.0.jar',
    './drivers/spark-hive/hive-shims-common-1.2.1.spark2.jar',
    './drivers/spark-hive/jersey-core-1.9.jar',
    './drivers/spark-hive/curator-recipes-2.6.0.jar',
    './drivers/spark-hive/jackson-jaxrs-1.8.3.jar',
    './drivers/spark-hive/hadoop-yarn-common-2.6.0.jar',
    './drivers/spark-hive/hadoop-annotations-2.6.0.jar',
    './drivers/spark-hive/junit-3.8.1.jar',
    './drivers/spark-hive/commons-pool-1.5.4.jar',
    './drivers/spark-hive/bonecp-0.8.0.RELEASE.jar',
    './drivers/spark-hive/jline-0.9.94.jar',
  ]);
}

const clusterConfig = JSON.parse(fs.readFileSync('./cluster.json'));

var config = {
  // Required
  url: `jdbc:hive2://${clusterConfig[clusterConfig.use].host}:${clusterConfig[clusterConfig.use].port}/${clusterConfig[clusterConfig.use].db}`,

  drivername: clusterConfig[clusterConfig.use].driver,
  minpoolsize: clusterConfig[clusterConfig.use].minpoolsize,
  maxpoolsize: clusterConfig[clusterConfig.use].maxpoolsize,

  user: clusterConfig[clusterConfig.use].user,
  password: clusterConfig[clusterConfig.use].password,
  properties: {}
};

var hsqldb = new JDBC(config);

module.exports = (query, callback) => {
  hsqldb.reserve(function (err, connObj) {
    if (connObj) {
      console.log("Using connection: " + connObj.uuid);
      var conn = connObj.conn;

      conn.createStatement(function (err, statement) {
        if (err) {
          callback(true, HttpStatus.GATEWAY_TIMEOUT, err);
        } else {
          statement.setFetchSize(100, function (err) {
            if (err) {
              callback(true, HttpStatus.REQUEST_TIMEOUT, err);
            } else {
              statement.executeQuery(query, function (err, resultSet) {
                if (err) {
                  callback(true, HttpStatus.BAD_REQUEST, err)
                } else {
                  resultSet.toObjArray(function (err, results) {
                    callback(null, HttpStatus.OK, results);
                  });
                }
              });
            }
          });
        }
      });
    }
  });
};

// QUERY('SHOW TABLES', (resultSet) => {
//   console.log('-'.repeat(60));
//   console.log(resultSet);
//   console.log('-'.repeat(60));
// });
//
// QUERY('SELECT * FROM acu_sts_n_enb_v1', (resultSet) => {
//   console.log('-'.repeat(60));
//   console.log(resultSet);
//   console.log('-'.repeat(60));
// });