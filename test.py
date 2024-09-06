import os
from pyspark.sql import SparkSession

# Create a SparkSession with Hive support enabled
spark = SparkSession.builder \
    .appName("Hive Config File Info") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Print Spark version
print("Spark version: ", spark.version)

# Check and print Hive home directory from environment variables
hive_home = os.getenv('HIVE_HOME')
if hive_home:
    print(f"HIVE_HOME environment variable is set to: {hive_home}")
    # Check for hive-site.xml in the conf directory inside HIVE_HOME
    hive_site_path = os.path.join(hive_home, "conf", "hive-site.xml")
    if os.path.exists(hive_site_path):
        print(f"Hive configuration file found at: {hive_site_path}")
    else:
        print(f"hive-site.xml not found in: {hive_site_path}")
else:
    print("HIVE_HOME environment variable is not set.")

# Also, check for Spark configuration directory (which may contain Hive-related settings)
spark_conf_dir = os.getenv('SPARK_CONF_DIR')
if spark_conf_dir:
    print(f"SPARK_CONF_DIR environment variable is set to: {spark_conf_dir}")
    # Check for spark-defaults.conf if the SPARK_CONF_DIR is set
    spark_defaults_path = os.path.join(spark_conf_dir, "spark-defaults.conf")
    if os.path.exists(spark_defaults_path):
        print(f"Spark configuration file found at: {spark_defaults_path}")
    else:
        print(f"spark-defaults.conf not found in: {spark_conf_dir}")
else:
    print("SPARK_CONF_DIR environment variable is not set.")

# Print Hive-related Spark configurations
print("Hive-related Spark configurations:")
hive_conf = [conf for conf in spark.sparkContext.getConf().getAll() if 'hive' in conf[0].lower()]
for key, value in hive_conf:
    print(f"{key}: {value}")

# Stop the Spark session
spark.stop()
