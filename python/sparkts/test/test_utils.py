import unittest
from sparkts.utils import add_pyspark_path, quiet_py4j

add_pyspark_path()
quiet_py4j()

from pyspark.context import SparkContext

class PySparkTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        self.sc = SparkContext('local', class_name)
        self.sc._jvm.System.setProperty("spark.ui.showConsoleProgress", "false")
        log4j = self.sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    def tearDown(self):
        self.sc.stop()
        # To avoid Akka rebinding to the same port, since it doesn't unbind
        # immediately on shutdown
        self.sc._jvm.System.clearProperty("spark.driver.port")


