# MLOps-Level3
ML & MLOps - a practical 6-week track by appliedAI institute for Europe in collaboration with MFS and PEG. Over six weeks we will build an end-to-end machine learning system on tabular data, moving from data preparation and model training to experiment tracking and deployment. More info: https://level3.hn/tracks/mlops?track_run=3

During the track we will learn and build the data foundations (pipelines and orchestration), ML basics (implement and evaluate models), working with ML libraries (feature engineering and tuning), and MLOps practices (reproducible training, experiment tracking, model management, and serving). The program culminates in a real-world challenge with an industry partner to apply the full workflow.

Tools and expectations: the track uses Python and common tools for production-ready workflows - Pandas, Dagster, scikit-learn, XGBoost, MLflow, and LakeFS.

# Week 1 — Titanic classification

This folder contains the Week 1 assignment: a short supervised learning exercise using the Titanic passenger dataset. The work is split into two parts: (1) explore the data and train a logistic regression model with scikit-learn, and (2) implement logistic regression from scratch using NumPy to deepen understanding of the algorithm.

Notebooks you should open:

- [titanic_data_exploration.ipynb](week-1/titanic_data_exploration.ipynb) — exploratory data analysis, visualizations, and feature checks.
- [titanic_survival_using_built-in_libs.ipynb](week-1/titanic_survival_using_built-in_libs.ipynb) — train and evaluate a logistic regression model using scikit-learn.
- [titanic_survival_from_scratch.ipynb](week-1/titanic_survival_from_scratch.ipynb) — a NumPy implementation of logistic regression with a training loop and loss tracking.

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

What was done in Week 1
- Explored the Titanic dataset to identify missing values, categorical variables, and useful features (e.g., `Pclass`, `Gender`, `Age`, `Fare`).
- Preprocessed data (simple imputation, encoding categorical features, and creating train/test splits).
- Trained a logistic regression classifier with scikit-learn and evaluated performance using accuracy, precision, recall, and F1-score.
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

# Week 2

Week 2 — Data pipeline for the Bike Rental project

In week 2 the focus is on data engineering: building a reproducible preprocessing pipeline using Dagster. The pipeline combines multiple CSV sources (booked rentals, direct pickups, weather, holidays), converts event-level records to hourly aggregates, enriches with weather and holiday information, and produces a final preprocessed CSV for later ML work.

Where to look
- Assignment and background: [week-2/project_handout/README.md](week-2/project_handout/README.md)
- Dagster assets, resources and definitions: [week-2/src/bike_rental](week-2/src/bike_rental)
- Week 2 quickstart and notes: [week-2/README.md](week-2/README.md)

Quickstart
- Install project dependencies: `uv sync` (from the project root)
- Start Dagster dev server for the Week 2 folder (from `week-2`):

```bash
uv run dg dev
```

Open the UI at http://localhost:3000 and materialize assets from the Assets view.

Key design notes
- External I/O and file paths are centralized in a `ProjectConfig` resource and a `CSVIO` resource handles CSV read/write. This makes the pipeline easier to configure, test, and swap to other backends (S3, DB) later.
- Assets accept resources by direct injection (e.g. `def bike_rental_hourly(csv_io, project_config)`), so Dagster will provide the configured resources at runtime.

Data files
Place the input CSVs in `week-2/data/`:
- `registered_bike_rentals.csv`
- `direct_pickup_bike_rentals.csv`
- `weather.csv`
- `holidays.csv`