# Week 1 — Titanic classification

This folder contains the Week 1 assignment: a short supervised learning exercise using the Titanic passenger dataset. The work is split into two parts: (1) explore the data and train a logistic regression model with scikit-learn, and (2) implement logistic regression from scratch using NumPy to deepen understanding of the algorithm.

Notebooks you should open:

- [titanic_data_exploration.ipynb](titanic_data_exploration.ipynb) — exploratory data analysis, visualizations, and feature checks.
- [titanic_survival_using_built-in_libs.ipynb](titanic_survival_using_built-in_libs.ipynb) — train and evaluate a logistic regression model using scikit-learn.
- [titanic_survival_from_scratch.ipynb](titanic_survival_from_scratch.ipynb) — a NumPy implementation of logistic regression with a training loop and loss tracking.

Quick start

1. Install dependencies from the project root:

```bash
uv sync
```

2. Start a Jupyter server or open the notebooks in VS Code:

```bash
# start Jupyter in this folder
jupyter lab
```

What I did in this project
- Explored the Titanic dataset to identify missing values, categorical variables, and useful features (e.g., `Pclass`, `Gender`, `Age`, `Fare`).
- Preprocessed data (simple imputation, encoding categorical features, and creating train/test splits).
- Trained a logistic regression classifier with scikit-learn and evaluated performance using accuracy, precision, recall, F1-score.
- Implemented logistic regression from scratch in NumPy: linear model, sigmoid activation, cross-entropy loss, gradients, and a simple gradient-descent training loop.
- Compared the from-scratch implementation with scikit-learn results to validate correctness.

Key learnings
- How to structure exploratory data analysis: check distributions, missingness, and correlations before modeling.
- Practical preprocessing steps for tabular data: imputation, categorical encoding, and feature scaling.
- How logistic regression works internally: the role of the sigmoid, the cross-entropy loss, and gradient-based parameter updates.
- How to evaluate classification models using multiple metrics and interpret their trade-offs.

Notes and next steps
- If you want to turn this into a repeatable pipeline, consider moving preprocessing and training into Python modules and orchestration (Dagster) similar to Week 2.
- Ideas to extend the assignment: add regularization, try other models (random forest, XGBoost), or perform cross-validation and hyperparameter search.
