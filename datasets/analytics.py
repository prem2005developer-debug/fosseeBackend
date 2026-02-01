import pandas as pd
import numpy as np
import math

def _py(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        x = float(v)
        if math.isfinite(x):
            return x
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    if isinstance(v, (list, tuple)):
        return [_py(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _py(val) for k, val in v.items()}
    return v

def _json_safe(obj):
    return _py(obj)

def _pretty_edges_labels(edges, decimals=0, sep="–"):
    """
    edges: list/np array of bin edges length = bins+1
    returns labels: ["a–b", ...]
    """
    labels = []
    for i in range(len(edges) - 1):
        a = round(float(edges[i]), decimals)
        b = round(float(edges[i + 1]), decimals)
        if decimals == 0:
            a = int(a)
            b = int(b)
        labels.append(f"{a}{sep}{b}")
    return labels

def _hist_counts(series: pd.Series, bins=5):
    """
    Returns (edges, counts) using numpy histogram.
    """
    s = series.dropna()
    if s.empty:
        return None, [0] * bins

    counts, edges = np.histogram(s.to_numpy(dtype=float), bins=bins)
    return edges, counts.tolist()

def _stats(df: pd.DataFrame, col: str):
    s = df[col].dropna()
    if s.empty:
        return {
            "count": 0,
            "mean": None,
            "std": None,
            "min": None,
            "q1": None,
            "median": None,
            "q3": None,
            "max": None,
        }
    return {
        "count": int(s.count()),
        "mean": float(s.mean()),
        "std": float(s.std(ddof=1)) if s.count() > 1 else 0.0,
        "min": float(s.min()),
        "q1": float(s.quantile(0.25)),
        "median": float(s.median()),
        "q3": float(s.quantile(0.75)),
        "max": float(s.max()),
    }

def _iqr_outliers(series: pd.Series):
    s = series.dropna()
    if s.empty:
        return {"min": None, "q1": None, "median": None, "q3": None, "max": None, "outliers": []}

    q1 = s.quantile(0.25)
    med = s.median()
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    outs = s[(s < low) | (s > high)].tolist()

    return {
        "min": float(s.min()),
        "q1": float(q1),
        "median": float(med),
        "q3": float(q3),
        "max": float(s.max()),
        "outliers": [float(x) for x in outs],
    }

def _pressure_boxplot_each_equipment(df: pd.DataFrame):
    """
    Boxplot-ready structure for PRESSURE distribution for each equipment NAME.

    Returns:
      {
        "labels": ["Pump-1","Pump-2",...],
        "values": [
          [min,q1,median,q3,max],  # Pump-1 pressure distribution
          [min,q1,median,q3,max],  # Pump-2 pressure distribution
          ...
        ]
      }

    Notes:
    - If each equipment appears only once, boxplot becomes degenerate:
      min=q1=median=q3=max=that single value.
    - If equipment repeats across time/uploads, this becomes a real distribution.
    """
    labels = []
    values = []

    for name, g in df.groupby("name", dropna=True):
        s = g["pressure"].dropna()
        if s.empty:
            continue

        q = s.quantile([0, 0.25, 0.5, 0.75, 1]).tolist()
        labels.append(str(name))
        values.append([float(x) for x in q])

    paired = sorted(zip(labels, values), key=lambda x: x[0].lower())
    labels = [p[0] for p in paired]
    values = [p[1] for p in paired]

    return {"labels": labels, "values": values}

def _pressure_boxplot_by_type(df: pd.DataFrame):
    labels = []
    values = []

    for t, g in df.groupby("type", dropna=True):
        s = g["pressure"].dropna()
        if s.empty:
            continue
        q = s.quantile([0, 0.25, 0.5, 0.75, 1]).tolist()
        labels.append(str(t))
        values.append([float(x) for x in q])

    paired = sorted(zip(labels, values), key=lambda x: x[0].lower())
    return {"labels": [p[0] for p in paired], "values": [p[1] for p in paired]}

def _series_list(series: pd.Series, max_points: int | None = None):
    """
    Returns a list of floats (or None for missing).
    If max_points is provided and data is larger, downsample uniformly.
    """
    s = series.copy()
    arr = [None if pd.isna(x) else float(x) for x in s.tolist()]

    if max_points is None or len(arr) <= max_points:
        return arr

    idx = np.linspace(0, len(arr) - 1, max_points).astype(int)
    return [arr[i] for i in idx]

def analyze_equipment_json(records: list):
    df = pd.DataFrame(records)

    df = df.rename(columns={
        "Equipment Name": "name",
        "Type": "type",
        "Flowrate": "flowrate",
        "Pressure": "pressure",
        "Temperature": "temperature",
    })

    for col in ["name", "type", "flowrate", "pressure", "temperature"]:
        if col not in df.columns:
            df[col] = None

    for col in ["flowrate", "pressure", "temperature"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    total_count = int(len(df))

    avg_flowrate = df["flowrate"].mean()
    avg_pressure = df["pressure"].mean()
    avg_temperature = df["temperature"].mean()

    type_distribution = df["type"].value_counts(dropna=True).to_dict()

    clean_scatter = df.dropna(subset=["flowrate", "pressure", "temperature"])
    if len(clean_scatter) > 0:
        sample_df = clean_scatter.sample(n=min(200, len(clean_scatter)), random_state=42)
    else:
        sample_df = clean_scatter

    scatter_points = sample_df.apply(
        lambda r: {
            "x": round(r["flowrate"], 2),
            "y": round(r["pressure"], 2),
            "t": round(r["temperature"], 2),
        },
        axis=1
    ).tolist()

    bins = 5

    flow_edges, flow_counts = _hist_counts(df["flowrate"], bins=bins)
    temp_edges, temp_counts = _hist_counts(df["temperature"], bins=bins)

    edges_for_labels = flow_edges if flow_edges is not None else temp_edges

    histogram = {
        "labels": _pretty_edges_labels(edges_for_labels, decimals=0) if edges_for_labels is not None else [],
        "flowrate": flow_counts,
        "temperature": temp_counts,
    }

    boxplot = {
        "labels": ["Flowrate", "Pressure", "Temperature"],
        "values": [
            df["flowrate"].quantile([0, .25, .5, .75, 1]).tolist(),
            df["pressure"].quantile([0, .25, .5, .75, 1]).tolist(),
            df["temperature"].quantile([0, .25, .5, .75, 1]).tolist(),
        ],
    }

    PressureBoxplotByEquipment = _pressure_boxplot_by_type(df)

    corr = df[["flowrate", "pressure", "temperature"]].corr()
    correlation = [
        {
            "x": i.capitalize() if i != "flowrate" else "Flowrate",
            "y": j.capitalize() if j != "flowrate" else "Flowrate",
            "v": round(float(corr.loc[i, j]), 4) if pd.notna(corr.loc[i, j]) else 0.0
        }
        for i in corr.columns
        for j in corr.columns
    ]

    statistical_summary = {
        "data": {
            "flowrate": _stats(df, "flowrate"),
            "pressure": _stats(df, "pressure"),
            "temperature": _stats(df, "temperature"),
        }
    }

    grouped = {}
    for t, g in df.groupby("type", dropna=True):
        grouped[str(t)] = {
            "flowrate": _stats(g, "flowrate"),
            "pressure": _stats(g, "pressure"),
            "temperature": _stats(g, "temperature"),
        }

    dist_stats = _iqr_outliers(df["flowrate"])
    DistributionAnalysis = {
        "title": "Flowrate",
        "unit": " m³/h",
        "stats": dist_stats
    }

    corr2 = df[["flowrate", "pressure", "temperature"]].corr().fillna(0.0)
    CorrelationInsights = {
        "matrix": {
            "Flowrate": {
                "Flowrate": 1.0,
                "Pressure": float(corr2.loc["flowrate", "pressure"]),
                "Temperature": float(corr2.loc["flowrate", "temperature"]),
            },
            "Pressure": {
                "Flowrate": float(corr2.loc["pressure", "flowrate"]),
                "Pressure": 1.0,
                "Temperature": float(corr2.loc["pressure", "temperature"]),
            },
            "Temperature": {
                "Flowrate": float(corr2.loc["temperature", "flowrate"]),
                "Pressure": float(corr2.loc["temperature", "pressure"]),
                "Temperature": 1.0,
            },
        }
    }

    avg_p = df["pressure"].mean()
    cond_df = df[df["pressure"] > avg_p] if pd.notna(avg_p) else df.iloc[0:0]
    ConditionalAnalysis = {
        "conditionLabel": "Records with ABOVE average pressure",
        "totalRecords": int(len(cond_df)),
        "stats": {
            "flowrate": float(cond_df["flowrate"].mean()) if len(cond_df) else None,
            "pressure": float(cond_df["pressure"].mean()) if len(cond_df) else None,
            "temperature": float(cond_df["temperature"].mean()) if len(cond_df) else None,
        }
    }

    EquipmentPerformanceRanking = {}
    for t, g in df.groupby("type", dropna=True):
        EquipmentPerformanceRanking[str(t)] = {
            "flowrate": float(g["flowrate"].mean()) if len(g) else None,
            "pressure": float(g["pressure"].mean()) if len(g) else None,
            "temperature": float(g["temperature"].mean()) if len(g) else None,
        }

    SeriesData = {
        "flowrate": _series_list(df["flowrate"], max_points=None),
        "temperature": _series_list(df["temperature"], max_points=None)
    }
    
    preview = df[["name", "type", "flowrate", "pressure", "temperature"]].head(20).to_dict(orient="records")

    result = {
        "total_count": total_count,
        "avg_flowrate": avg_flowrate,
        "avg_pressure": avg_pressure,
        "avg_temperature": avg_temperature,
        "type_distribution": type_distribution,

        "scatter_points": scatter_points,
        "histogram": histogram,
        "boxplot": PressureBoxplotByEquipment,
        "correlation": correlation,

        "StatisticalSummary": statistical_summary,
        "GroupedEquipmentAnalytics": grouped,
        "SeriesData": SeriesData,
        "DistributionAnalysis": DistributionAnalysis,
        "CorrelationInsights": CorrelationInsights,
        "ConditionalAnalysis": ConditionalAnalysis,
        "EquipmentPerformanceRanking": EquipmentPerformanceRanking,

        "data": preview,
    }
    return _json_safe(result)