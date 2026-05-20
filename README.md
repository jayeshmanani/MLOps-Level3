# MLOps-Level3
ML & MLOps - a practical 6-week, a track by appliedAI institure for europe in collaboration with MFS and PEG. Over six weeks we will build an end-to-end machine learning system on tabular data, moving from data preparation and model training to experiment tracking and deployment. More info: https://level3.hn/tracks/mlops?track_run=3

During the track we will learn and build the data foundations (pipelines and orchestration), ML basics (implement and evaluate models), working with ML libraries (feature engineering and tuning), and MLOps practices (reproducible training, experiment tracking, model management, and serving). The program culminates in a real-world challenge with an industry partner to apply the full workflow.

Tools and expectations: the track uses Python and common tools for production-ready workflows - Pandas, Dagster, scikit-learn, XGBoost, MLflow, and LakeFS.

# Week 1

Week 1 - Classification model

In week 1 the focus in on building first supervised machine learning model using the Titanic passenger dataset. The project is split into two parts: first train and evaluate a logistic regression classifier with scikit-learn, then implement logistic regression from scratch with NumPy to understand the algorithmic building blocks (linear score, sigmoid, loss, gradients, and the training loop).

Core activities: explore and clean the dataset, create simple feature transformations and visualizations, used feature engineering techniques such as mutual information to identify the relationship between the survival which is our target in this dataset and the other columns (passenger_class, age, gender, fare, family_size), train and evaluate models with multiple metrics such as Accuracy, Recall, Precision, and F1 Score, and verify a from-scratch implementation by tracking loss during training. Deliverables are executable Jupyter notebooks, clear markdown explanations, and reproducible runs that demonstrate decreasing training loss and sensible evaluation scores.

