import os
import pickle
from typing import Any, Union


def save_trained_model(model_path: str, model: Any):
    """Save trained model into pickle format

    Args:
        model_path (str): Path to save model
        model (Any): Model to save
    """
    with open(model_path, "wb") as file:
        pickle.dump(model, file)


def load_trained_model(model_path: str) -> Union[Any, None]:
    """Load trained model

    Args:
        model_path (str): Path to save model

    Returns:
        Union[Any, None]: Trained model or None if not found
    """
    if not os.path.isfile(model_path):
        return None

    with open(model_path, "rb") as file:
        return pickle.load(file)
