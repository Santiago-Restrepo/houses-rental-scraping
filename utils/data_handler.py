import pandas as pd

def save_data_to_csv(data: list, filename: str):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

def read_data_from_csv(filename: str):
    return pd.read_csv(filename)
