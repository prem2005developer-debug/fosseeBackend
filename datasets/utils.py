import pandas as pd


def analyze_csv(file_path):
    df = pd.read_csv(file_path)

    summary = {
        "total_rows": len(df),
        "average_flowrate": df["Flowrate"].mean(),
        "average_pressure": df["Pressure"].mean(),
        "average_temperature": df["Temperature"].mean(),
        "equipment_distribution": df["Type"].value_counts().to_dict()
    }

    return summary, df.head(20).to_dict(orient="records")
