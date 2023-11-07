import luigi
from data import SentimentAnalysisZipFile


class TestRawData(luigi.Task):
    """
    Test raw data
    """

    def requires(self):
        return SentimentAnalysisZipFile()

    def output(self):
        return luigi.LocalTarget("../data/tmp/test_raw_data.txt")

    def run(self):
        with self.input().open() as f:
            pass
        with self.output().open("w") as f:
            f.write("SUCCESS")


class SampleDVCFile(luigi.ExternalTask):
    """
    Raw data zip file
    """

    def output(self):
        return luigi.LocalTarget("../data/data.xml.dvc")


class SentimentAnalysisDVCFile(luigi.ExternalTask):
    """
    Raw data zip file
    """

    def output(self):
        return luigi.LocalTarget("../data/sentiment-analysis-on-movie-reviews.zip.dvc")


class TestDVCFiles(luigi.Task):
    """
    Test dvc files
    """

    def requires(self):
        return [SampleDVCFile(), SentimentAnalysisDVCFile()]

    def output(self):
        return luigi.LocalTarget("../data/tmp/test_dvc_files.txt")

    def run(self):
        self.input()
        with self.output().open("w") as f:
            f.write("SUCCESS")


class Tests(luigi.WrapperTask):
    """
    Run all tests
    """

    def requires(self):
        return [TestRawData(), TestDVCFiles()]
