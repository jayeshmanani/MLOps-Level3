"""Baseline model for bike rental prediction."""

import dagster as dg
from sklearn.metrics import mean_absolute_error


@dg.asset(group_name="baseline_model")
def baseline_previous_value_model(context, X_train, y_train):
    """Baseline model that uses the mean of the.

    training target values as the prediction.
    """
    baseline_value = y_train.mean()

    context.add_output_metadata(
        {
            "baseline_type": "mean",
            "value": float(baseline_value),
        }
    )

    return baseline_value


@dg.asset(group_name="baseline_model")
def baseline_metrics(context, baseline_previous_value_model, X_test, y_test):
    """Evaluate the baseline model using MAE."""
    preds = [baseline_previous_value_model] * len(y_test)

    mae = mean_absolute_error(y_test, preds)

    context.add_output_metadata({"mae": float(mae), "model": "mean_baseline"})

    return {"mae": mae}


@dg.asset(group_name="baseline_model")
def baseline_yesterday_model(context, clean_curated_data):
    """Baseline model that uses the previous day's rental.

    count as the prediction for the current day.
    """
    df = clean_curated_data.sort_values("datetime").copy()

    df = df.dropna(subset=["total_count"])

    test_size = int(len(df) * 0.3)

    train = df.iloc[:-test_size]
    test = df.iloc[-test_size:].copy()

    # --- baseline: yesterday value ---
    test["baseline_pred"] = test["total_count"].shift(1)

    # fix first row safely
    test.iloc[0, test.columns.get_loc("baseline_pred")] = train[
        "total_count"
    ].iloc[-1]

    # drop NaNs before metric
    valid = test.dropna(subset=["baseline_pred", "total_count"])

    mae = mean_absolute_error(valid["total_count"], valid["baseline_pred"])

    context.add_output_metadata(
        {
            "baseline": "yesterday_value",
            "mae": float(mae),
            "rows_evaluated": len(valid),
        }
    )

    return mae
