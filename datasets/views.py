from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework.permissions import AllowAny
from django.utils.timezone import localtime

from .models import Dataset
from .analytics import analyze_equipment_json
import logging

class UploadCSVView(APIView):
    parser_classes = [JSONParser]
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            data = request.data

            if not isinstance(data, list):
                return Response(
                    {"error": "Expected a JSON array of equipment records"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            if len(data) == 0:
                return Response(
                    {"error": "Dataset cannot be empty"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            required_keys = {
                "Equipment Name",
                "Type",
                "Flowrate",
                "Pressure",
                "Temperature",
            }

            for idx, row in enumerate(data):
                if not isinstance(row, dict):
                    return Response(
                        {"error": f"Row {idx} is not a valid object"},
                        status=status.HTTP_400_BAD_REQUEST
                    )

                missing = required_keys - row.keys()
                if missing:
                    return Response(
                        {"error": f"Row {idx} missing fields", "missing": list(missing)},
                        status=status.HTTP_400_BAD_REQUEST
                    )

            try:
                summary = analyze_equipment_json(data)
            except Exception as e:
                logging.exception("Error analyzing equipment JSON")
                return Response(
                    {"error": "Failed to analyze dataset", "details": str(e)},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            try:
                dataset = Dataset.objects.create(
                    name=f"dataset_{Dataset.objects.count() + 1}",
                    raw_data=data,
                    summary=summary,
                )
            except Exception as e:
                logging.exception("Error saving dataset to database")
                return Response(
                    {"error": "Failed to save dataset", "details": str(e)},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            try:
                old_datasets = Dataset.objects.order_by("-uploaded_at")[5:]
                if old_datasets.exists():
                    Dataset.objects.filter(pk__in=[d.pk for d in old_datasets]).delete()
            except Exception:
                logging.exception("Error deleting old datasets")

            return Response(
                {"id": dataset.id, **summary},
                status=status.HTTP_201_CREATED
            )
        except Exception as e:
            logging.exception("Unexpected error in UploadCSVView")
            return Response(
                {"error": "An unexpected error occurred", "details": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

logger = logging.getLogger(__name__)

def _to_float(x):
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None

def _normalize_equipment_record(row: dict):
    if not isinstance(row, dict):
        return None

    name = (
        row.get("name")
        or row.get("Equipment Name")
        or row.get("equipment_name")
        or row.get("EquipmentName")
    )
    typ = row.get("type") or row.get("Type") or row.get("equipment_type")

    flow = row.get("flowrate") or row.get("Flowrate") or row.get("Flow Rate") or row.get("flow_rate")
    press = row.get("pressure") or row.get("Pressure")
    temp = row.get("temperature") or row.get("Temperature")

    return {
        "Equipment Name": "" if name is None else str(name),
        "Type": "" if typ is None else str(typ),
        "Flowrate": _to_float(flow),
        "Pressure": _to_float(press),
        "Temperature": _to_float(temp),
    }

def _ensure_charts_grid_shape(dataset_id: int, summary: dict, fallback_total: int):
    s = summary if isinstance(summary, dict) else {}

    out = {"id": int(dataset_id), **s}

    out.setdefault("total_count", int(out.get("total_count") or fallback_total))
    out.setdefault("avg_flowrate", out.get("avg_flowrate", None))
    out.setdefault("avg_pressure", out.get("avg_pressure", None))
    out.setdefault("avg_temperature", out.get("avg_temperature", None))

    out.setdefault("type_distribution", out.get("type_distribution") or {})

    out.setdefault("scatter_points", out.get("scatter_points") or [])
    out.setdefault("histogram", out.get("histogram") or {"labels": [], "flowrate": [], "temperature": []})
    out.setdefault("boxplot", out.get("boxplot") or {"labels": [], "values": []})
    out.setdefault("correlation", out.get("correlation") or [])

    out.setdefault("StatisticalSummary", out.get("StatisticalSummary") or {"data": {}})
    out.setdefault("GroupedEquipmentAnalytics", out.get("GroupedEquipmentAnalytics") or {})

    out.setdefault("DistributionAnalysis", out.get("DistributionAnalysis") or {
        "title": "Flowrate",
        "unit": " m³/h",
        "stats": {"min": None, "q1": None, "median": None, "q3": None, "max": None, "outliers": []}
    })
    out.setdefault("CorrelationInsights", out.get("CorrelationInsights") or {"matrix": {}})
    out.setdefault("ConditionalAnalysis", out.get("ConditionalAnalysis") or {
        "conditionLabel": "Records with ABOVE average pressure",
        "totalRecords": 0,
        "stats": {"flowrate": None, "pressure": None, "temperature": None},
    })
    out.setdefault("EquipmentPerformanceRanking", out.get("EquipmentPerformanceRanking") or {})

    out.setdefault("data", out.get("data") or [])
    return out

class DatasetHistoryView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        try:
            limit_raw = request.query_params.get("limit", 5)
            limit = int(limit_raw)
        except (TypeError, ValueError):
            return Response(
                {"error": "Invalid limit parameter. Must be an integer."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if limit < 1:
            limit = 1
        if limit > 5:
            limit = 5

        try:
            qs = Dataset.objects.order_by("-uploaded_at")[:limit]
        except Exception as e:
            logger.exception("Failed to query dataset history")
            return Response(
                {"error": f"An error occurred while retrieving datasets. {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        datasets_obj = {}
        order = []

        for d in qs:
            try:
                raw = d.raw_data if isinstance(getattr(d, "raw_data", None), list) else []
                normalized = []
                for row in raw:
                    rec = _normalize_equipment_record(row)
                    if rec is not None:
                        normalized.append(rec)

                summary = d.summary if isinstance(getattr(d, "summary", None), dict) else {}
                dataset_payload = _ensure_charts_grid_shape(d.id, summary, fallback_total=len(normalized))

                meta = {
                    "name": getattr(d, "name", None),
                    "uploaded_at": localtime(d.uploaded_at).isoformat() if getattr(d, "uploaded_at", None) else None,
                }

                if getattr(d, "file", None):
                    try:
                        meta["file_size"] = d.file.size
                    except Exception:
                        meta["file_size"] = None

                datasets_obj[str(d.id)] = {
                    "dataset": dataset_payload,
                    "data": normalized,
                    "meta": meta,
                }
                order.append(int(d.id))

            except Exception:
                logger.exception("Failed to serialize dataset %s", getattr(d, "id", "unknown"))
                continue

        return Response(
            {
                "count": len(order),
                "order": order,
                "datasets": datasets_obj
            },
            status=status.HTTP_200_OK,
        )