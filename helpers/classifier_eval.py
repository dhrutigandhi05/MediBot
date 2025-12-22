from typing import Dict, Any, Tuple
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score

def train_eval_classifier(clf, X, y, test_size: float = 0.15, random_seed: int = 42) -> Tuple[Any, Dict[str, Any]]:
    # split into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_seed, stratify=y
    )

    clf.fit(X_train, y_train) # train the classifier

    y_pred = clf.predict(X_test) # predict labels on the test set
    y_proba = clf.predict_proba(X_test) # predict probabilities on the test set
    classes = list(clf.named_steps["logreg"].classes_) # extract the classes from the logistic regression step

    # collect evaluation metrics
    metrics: Dict[str, Any] = {
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "classes": classes,
        "report_text": classification_report(y_test, y_pred, digits=4),
        "confusion_matrix": confusion_matrix(y_test, y_pred, labels=classes).tolist(),
    }

    # add roc_auc score for drug class
    if "drug" in classes:
        drug_idx = classes.index("drug")
        y_bin = [1 if yy == "drug" else 0 for yy in y_test]
        metrics["roc_auc_drug"] = float(roc_auc_score(y_bin, y_proba[:, drug_idx]))

    return clf, metrics