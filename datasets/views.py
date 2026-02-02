import json
import logging

from django.utils.timezone import localtime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import JSONParser
from rest_framework.permissions import AllowAny

from .models import Dataset
from .analytics import analyze_equipment_json

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


def _get_first_available(row: dict, keys: list):
    for key in keys:
        if key in row:
            return row[key]
    return None


def _normalize_equipment_record(row: dict):
    if not isinstance(row, dict):
        return None

    name = _get_first_available(
        row, ["Equipment Name", "name", "equipment_name", "EquipmentName", "equipmentName"]
    )
    typ = _get_first_available(row, ["Type", "type", "equipment_type", "equipmentType"])
    flow = _get_first_available(row, ["Flowrate", "flowrate", "Flow Rate", "flow_rate", "flowRate"])
    press = _get_first_available(row, ["Pressure", "pressure"])
    temp = _get_first_available(row, ["Temperature", "temperature"])

    name_str = "" if name is None else str(name).strip()
    typ_str = "" if typ is None else str(typ).strip()

    return {
        "Equipment Name": name_str,
        "Type": typ_str,
        "Flowrate": _to_float(flow),
        "Pressure": _to_float(press),
        "Temperature": _to_float(temp),
    }


def _parse_jsonish(value):
    """
    Accepts list/dict as-is. If string, tries json.loads.
    Otherwise returns None.
    """
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def _ensure_charts_grid_shape(dataset_id: int, summary: dict, fallback_total: int):
    s = summary if isinstance(summary, dict) else {}
    out = {"id": int(dataset_id), **s}

    out["total_count"] = int(out.get("total_count") or fallback_total)
    out.setdefault("avg_flowrate", None)
    out.setdefault("avg_pressure", None)
    out.setdefault("avg_temperature", None)

    out.setdefault("type_distribution", {})

    out.setdefault("scatter_points", [])
    out.setdefault("histogram", {"labels": [], "flowrate": [], "temperature": []})
    out.setdefault("boxplot", {"labels": [], "values": []})
    out.setdefault("correlation", [])

    out.setdefault("StatisticalSummary", {"data": {}})
    out.setdefault("GroupedEquipmentAnalytics", {})

    out.setdefault(
        "DistributionAnalysis",
        {
            "title": "Flowrate",
            "unit": " m³/h",
            "stats": {"min": None, "q1": None, "median": None, "q3": None, "max": None, "outliers": []},
        },
    )
    out.setdefault("CorrelationInsights", {"matrix": {}})
    out.setdefault(
        "ConditionalAnalysis",
        {
            "conditionLabel": "Records with ABOVE average pressure",
            "totalRecords": 0,
            "stats": {"flowrate": None, "pressure": None, "temperature": None},
        },
    )
    out.setdefault("EquipmentPerformanceRanking", {})

    out.setdefault("data", [])
    return out


class UploadCSVView(APIView):
    parser_classes = [JSONParser]
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            data = request.data

            if not isinstance(data, list):
                return Response(
                    {"error": "Expected a JSON array of equipment records"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if not data:
                return Response(
                    {"error": "Dataset cannot be empty"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            normalized = []
            errors = []
            for idx, row in enumerate(data):
                if not isinstance(row, dict):
                    errors.append({"row": idx, "error": "Not a valid object"})
                    continue

                rec = _normalize_equipment_record(row)
                if rec is None:
                    errors.append({"row": idx, "error": "Could not be parsed"})
                    continue

                missing = [
                    field
                    for field in ["Equipment Name", "Type", "Flowrate", "Pressure", "Temperature"]
                    if rec.get(field) is None or (isinstance(rec.get(field), str) and not rec.get(field).strip())
                ]

                if missing:
                    errors.append({"row": idx, "error": "Missing/invalid fields", "missing": missing})
                    continue

                normalized.append(rec)

            if errors:
                return Response(
                    {"error": "Validation failed for some records", "details": errors},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            try:
                summary = analyze_equipment_json(normalized)
            except Exception as e:
                logger.exception("Error analyzing equipment JSON")
                return Response(
                    {"error": "Failed to analyze dataset", "details": str(e)},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            try:
                dataset = Dataset.objects.create(
                    name=f"dataset_{localtime().strftime('%Y%m%d_%H%M%S')}",
                    raw_data=json.dumps(normalized),
                    summary=json.dumps(summary),
                )
            except Exception as e:
                logger.exception("Error saving dataset to database")
                return Response(
                    {"error": "Failed to save dataset", "details": str(e)},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            try:
                # Keep the latest 5 datasets, delete older ones.
                pks_to_keep = list(
                    Dataset.objects.order_by("-uploaded_at").values_list("pk", flat=True)[:5]
                )
                Dataset.objects.exclude(pk__in=pks_to_keep).delete()
            except Exception:
                logger.exception("Error deleting old datasets")

            return Response({"id": dataset.id, **summary}, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.exception("Unexpected error in UploadCSVView")
            return Response(
                {"error": "An unexpected error occurred", "details": str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


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

        limit = max(1, min(5, limit))

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
                raw_value = getattr(d, "raw_data", None)
                raw_parsed = _parse_jsonish(raw_value)
                raw_list = raw_parsed if isinstance(raw_parsed, list) else []

                normalized = []
                for row in raw_list:
                    rec = _normalize_equipment_record(row)
                    if rec is not None:
                        normalized.append(rec)

                summary_value = getattr(d, "summary", None)
                summary_parsed = _parse_jsonish(summary_value)
                summary = summary_parsed if isinstance(summary_parsed, dict) else {}

                if (not summary) or int(summary.get("total_count") or 0) == 0:
                    if normalized:
                        try:
                            summary = analyze_equipment_json(normalized)
                        except Exception:
                            logger.exception("Failed to re-analyze dataset %s", d.id)
                            summary = summary or {}

                dataset_payload = _ensure_charts_grid_shape(
                    d.id, summary, fallback_total=len(normalized)
                )
                dataset_payload["data"] = normalized

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

        return Response({"count": len(order), "order": order, "datasets": datasets_obj}, status=status.HTTP_200_OK)
