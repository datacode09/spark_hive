val spark = SparkSession.builder
  .appName("Spark with Hive and Kerberos")
  .config("hive.metastore.uris", "thrift://hive-metastore-host:9083")  // Replace with your Hive Metastore URI
  .config("hive.server2.authentication.kerberos.principal", "hive/_HOST@YOUR.REALM.COM")  // Replace with your principal
  .enableHiveSupport()
  .getOrCreate()
