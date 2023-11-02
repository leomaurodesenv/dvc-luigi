import os
import luigi
import zipfile


class ExtractRawData(luigi.Task):
    """
    Extract raw data from zip file
    """

    data_path = luigi.Parameter(
        default="../data/sentiment-analysis-on-movie-reviews.zip"
    )

    def output(self):
        return {
            "test": luigi.LocalTarget("../data/output/test.tsv.zip"),
            "train": luigi.LocalTarget("../data/output/train.tsv.zip"),
        }

    def run(self):
        # Check if data file exists
        assert os.path.exists(self.data_path)

        # Unzip data file
        with zipfile.ZipFile(self.data_path, "r") as zip_ref:
            zip_ref.extractall("../data/output/")
