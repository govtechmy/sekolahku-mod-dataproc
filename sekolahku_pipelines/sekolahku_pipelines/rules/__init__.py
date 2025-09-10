import os

def load_rules():
    DATASET_NAME = os.getenv("DATASET_NAME")
    if DATASET_NAME == "sekolahku":
        from . import sekolahku
        return sekolahku 
    else:
        raise ValueError(f"Unsupported dataset type: {DATASET_NAME}")
