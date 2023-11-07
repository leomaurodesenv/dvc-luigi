import luigi
import joblib
import pandas as pd
import scipy.sparse
from data import ExtractRawData
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neural_network import MLPClassifier


class Preprocessing(luigi.Task):
    """
    NLP Preprocessing
    """

    def requires(self):
        return ExtractRawData()

    def output(self):
        return {
            "X": luigi.LocalTarget("../data/output/preprocessing.npz"),
            "vectorizer": luigi.LocalTarget("../data/output/preprocessing.joblib"),
        }

    def run(self):
        dataset = self.input()
        train = pd.read_csv(dataset["train"].path, sep="\t")
        corpus = train["Phrase"]
        vectorizer = CountVectorizer(
            lowercase=True, ngram_range=(1, 2), max_features=10_000
        )
        X = vectorizer.fit_transform(corpus)
        # storing results
        scipy.sparse.save_npz(self.output()["X"].path, X)
        joblib.dump(vectorizer, self.output()["vectorizer"].path)


class TrainModel(luigi.Task):
    """
    Train model
    """

    def requires(self):
        return {
            "data": ExtractRawData(),
            "preprocessing": Preprocessing(),
        }

    def output(self):
        return luigi.LocalTarget("../data/output/model.joblib")

    def run(self):
        _input = self.input()
        X = scipy.sparse.load_npz(_input["preprocessing"]["X"].path)
        y = pd.read_csv(_input["data"]["train"].path, sep="\t")["Sentiment"]
        model = MLPClassifier(
            max_iter=500,
            hidden_layer_sizes=(512, 256),
            early_stopping=True,
            random_state=29,
            verbose=True,
        )
        model.fit(X, y)
        joblib.dump(model, self.output().path)


class Predict(luigi.Task):
    """
    Predict
    """

    def requires(self):
        return {
            "data": ExtractRawData(),
            "preprocessing": Preprocessing(),
            "model": TrainModel(),
        }

    def output(self):
        return luigi.LocalTarget("../data/output/submission.csv")

    def run(self):
        _input = self.input()
        model = joblib.load(_input["model"].path)
        vectorizer = joblib.load(_input["preprocessing"]["vectorizer"].path)
        test = pd.read_csv(_input["data"]["test"].path, sep="\t")
        # Predicting
        test_X = vectorizer.transform(test["Phrase"].fillna(""))
        test["Sentiment"] = model.predict(test_X)
        test[["PhraseId", "Sentiment"]].to_csv(self.output().path, index=False)
