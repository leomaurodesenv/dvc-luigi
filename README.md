# NLP Pipeline using DVC and Luigi

[![GitHub](https://img.shields.io/static/v1?label=Code&message=GitHub&color=blue&style=flat-square)](https://github.com/leomaurodesenv/dvc-luigi-nlp)
[![MIT license](https://img.shields.io/static/v1?label=License&message=MIT&color=blue&style=flat-square)](LICENSE)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/leomaurodesenv/dvc-luigi-nlp/continuous-integration.yml?label=Build&style=flat-square)](https://github.com/leomaurodesenv/dvc-luigi-nlp/actions/workflows/continuous-integration.yml)


This is a project study to create a NLP pipeline using DVC and Luigi. The pipeline consists of several tasks that process text data, including preprocessing, feature extraction, and model training. Each task is defined as a [Luigi task](https://luigi.readthedocs.io/), which allows for easy tracking of dependencies and parallel execution. The pipeline also uses [DVC](https://dvc.org/) to manage data versioning and ensure reproducibility. The resulting model can be used for text classification or other NLP tasks.

> Note: This project contains a top-50 solution on the competition.

<p align="center"><img src="./docs/submission-score.png"></p>

This is a learning repository about DVC Data Version Control and Luigi Pipelines

- luigi, dvc, pre-commit
-
- setup https://pre-commit.com/, https://pre-commit.com/hooks.html
- setup https://github.com/Kaggle/kaggle-api
- `kaggle competitions download -c sentiment-analysis-on-movie-reviews -p data`
