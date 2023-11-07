import luigi
import zipfile


class SentimentAnalysisZipFile(luigi.ExternalTask):
    """
    Raw data zip file
    """

    def output(self):
        return luigi.LocalTarget("../data/sentiment-analysis-on-movie-reviews.zip")


class ExtractRawData(luigi.Task):
    """
    Extract raw data from zip file
    """

    def requires(self):
        return SentimentAnalysisZipFile()

    def output(self):
        return {
            "test": luigi.LocalTarget("../data/output/test.tsv.zip"),
            "train": luigi.LocalTarget("../data/output/train.tsv.zip"),
            "sampleSubission": luigi.LocalTarget("../data/output/sampleSubmission.csv"),
        }

    def run(self):
        # Unzip data file
        with zipfile.ZipFile(self.input().path, "r") as zip_ref:
            zip_ref.extractall("../data/output/")
