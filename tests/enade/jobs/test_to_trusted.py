import unittest, os
from pyspark.sql import SparkSession
from src.enade.jobs import to_trusted as T

class ToTrustedTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(ToTrustedTest, self).__init__(*args, **kwargs)

    def test_process(self):

        # Define files path
        path = os.path.dirname(__file__)
        fr_path = '{}/../locallake/raw/students.csv'.format(path)
        to_path = '{}/../locallake/trusted/students'.format(path)

        # Process data
        processor = T.ToTrusted(fr_path, to_path)
        processor.process()

        # Check saved data
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.format('delta').load(to_path)

        self.assertEqual(5, df.count())

        values = df.groupBy(['genre']).count().orderBy(['count']).collect()

        self.assertEqual(2, values[0]['count'])
        self.assertEqual(3, values[1]['count'])
