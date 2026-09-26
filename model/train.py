import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
import os
import joblib

# ─── CONFIG ───────────────────────────────────────────────────
DATA_PATH    = "data/transactions.csv"
MODEL_OUTPUT = "model/fraud_model"
SCALER_PATH  = "model/scaler.pkl"
EPOCHS       = 20
BATCH_SIZE   = 32
# ──────────────────────────────────────────────────────────────


def preprocess(df):
    le_txn     = LabelEncoder()
    le_country = LabelEncoder()

    df["txn_type_enc"] = le_txn.fit_transform(df["txn_type"])
    df["country_enc"]  = le_country.fit_transform(df["country"])

    features = ["amount", "hour", "txn_type_enc", "country_enc"]
    X = df[features].values
    y = df["is_fraud"].values

    scaler = StandardScaler()
    X      = scaler.fit_transform(X)

    joblib.dump(scaler,     SCALER_PATH)
    joblib.dump(le_txn,     "model/le_txn.pkl")
    joblib.dump(le_country, "model/le_country.pkl")

    return X, y


def build_model(input_dim):
    model = keras.Sequential([
        keras.layers.Dense(64,  activation="relu", input_shape=(input_dim,)),
        keras.layers.BatchNormalization(),
        keras.layers.Dropout(0.3),
        keras.layers.Dense(32,  activation="relu"),
        keras.layers.BatchNormalization(),
        keras.layers.Dropout(0.3),
        keras.layers.Dense(16,  activation="relu"),
        keras.layers.Dense(1,   activation="sigmoid"),
    ])

    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=0.001),
        loss="binary_crossentropy",
        metrics=["accuracy", keras.metrics.AUC(name="auc")],
    )
    return model


def train():
    df      = pd.read_csv(DATA_PATH)
    X, y    = preprocess(df)

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    model = build_model(X_train.shape[1])
    model.summary()

    callbacks = [
        keras.callbacks.EarlyStopping(patience=5, restore_best_weights=True),
        keras.callbacks.ReduceLROnPlateau(patience=3, factor=0.5),
    ]

    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        callbacks=callbacks,
        class_weight={0: 1, 1: 3},  # handle class imbalance
    )

    y_pred = (model.predict(X_val) > 0.5).astype(int)
    print(classification_report(y_val, y_pred, target_names=["legitimate", "fraud"]))
    print("Confusion Matrix:\n", confusion_matrix(y_val, y_pred))

    os.makedirs(MODEL_OUTPUT, exist_ok=True)
    model.save(MODEL_OUTPUT)
    print(f"Model saved to {MODEL_OUTPUT}")


if __name__ == "__main__":
    train()
