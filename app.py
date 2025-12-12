from dotenv import load_dotenv
import os
import time
from flask import Flask, jsonify, request
from flask_cors import CORS
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest,
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
)

from collections import deque
from datetime import datetime, timedelta

load_dotenv()

last_5min_active = deque()
BUFFER_DURATION = 5 * 60

# ---------- Configuration ----------
# Set env vars or edit here for quick local testing
SERVICE_ACCOUNT_FILE = os.getenv("GA4_SA_FILE", "service_account.json")
PROPERTY_NUM = os.getenv("GA4_PROPERTY_ID")  # numeric id, e.g. "123456789"
if not PROPERTY_NUM:
    raise RuntimeError("Set GA4_PROPERTY_ID environment variable (numeric property id)")

PROPERTY = f"properties/{PROPERTY_NUM}"
CACHE_TTL = int(os.getenv("GA4_CACHE_TTL_SEC", "5"))  # default small TTL to reduce API calls
# -----------------------------------

app = Flask(__name__)
CORS(app)

# Simple in-memory cache: { key: (expiry_ts, value) }
cache = {}

def get_cached(key):
    entry = cache.get(key)
    if not entry:
        return None
    expiry, value = entry
    if time.time() > expiry:
        cache.pop(key, None)
        return None
    return value

def set_cached(key, value, ttl=CACHE_TTL):
    cache[key] = (time.time() + ttl, value)

def get_ga_client():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    client = BetaAnalyticsDataClient(credentials=creds)
    return client

# ---------- Endpoint: Realtime - total active users in 30 mins----------
@app.route("/realtime-active", methods=["GET"])
def realtime_active():
    """
    Returns:
      { totalActive: int, fetchedAt: iso-string }
    """
    cache_key = "realtime-active"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        client = get_ga_client()
        request_body = RunRealtimeReportRequest(
            property=PROPERTY,
            metrics=[Metric(name="activeUsers")],
        )
        resp = client.run_realtime_report(request_body)

        # GA may return metric values aggregated in rows or as per-dimension;
        total = 0
        # If rows exist, sum them; otherwise metric_values at top-level may exist
        if resp.rows:
            for row in resp.rows:
                if row.metric_values:
                    try:
                        total += int(float(row.metric_values[0].value))
                    except Exception:
                        pass
        else:
            # fallback: check row_count or other fields
            # Many realtime responses place the activeUsers in a top-level metricValue in rows; handled above.
            total = 0

        out = {"totalActive": total, "fetchedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        set_cached(cache_key, out)
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    

# ---------- Endpoint: Realtime - pages by pageTitle ----------
'''
{
  "error": "400 Did you mean city? Field pageTitle is not a valid dimension. For a list of valid dimensions and metrics, see https://developers.google.com/analytics/devguides/reporting/data/v1/realtime-api-schema "
}
'''
@app.route("/realtime-pages", methods=["GET"])
def realtime_pages():
    """
    Returns:
      { rows: [ { pageTitle, activeUsers } ], fetchedAt }
    """
    cache_key = "realtime-pages"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        client = get_ga_client()
        request_body = RunRealtimeReportRequest(
            property=PROPERTY,
            dimensions=[Dimension(name="pageTitle")],
            metrics=[Metric(name="activeUsers")],
            limit=50,
        )
        resp = client.run_realtime_report(request_body)

        rows = []
        if resp.rows:
            for r in resp.rows:
                title = r.dimension_values[0].value if r.dimension_values else "(unknown)"
                value = 0
                if r.metric_values:
                    try:
                        value = int(float(r.metric_values[0].value))
                    except Exception:
                        value = 0
                rows.append({"pageTitle": title, "activeUsers": value})

        out = {"rows": rows, "fetchedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        set_cached(cache_key, out)
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Endpoint: Non-realtime - URLs (pageLocation) ----------
@app.route("/urls", methods=["GET"])
def urls_report():
    """
    Query params:
      start_date (default '7daysAgo'), end_date (default 'today')
    Returns:
      rows: [{ pageLocation, screenPageViews }]
    """
    start_date = request.args.get("start_date", "7daysAgo")
    end_date = request.args.get("end_date", "today")
    cache_key = f"urls:{start_date}:{end_date}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        client = get_ga_client()
        # Build a RunReportRequest
        rr = RunReportRequest(
            property=PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="pageLocation")],
            metrics=[Metric(name="screenPageViews")],
            limit=1000,
        )
        resp = client.run_report(rr)

        rows = []
        for r in resp.rows:
            page = r.dimension_values[0].value if r.dimension_values else "(unknown)"
            pv = 0
            if r.metric_values:
                try:
                    pv = int(float(r.metric_values[0].value))
                except Exception:
                    pv = 0
            rows.append({"pageLocation": page, "screenPageViews": pv})

        out = {"rows": rows, "rowCount": len(rows), "fetchedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        set_cached(cache_key, out, ttl=30)  # slightly longer cache for non-realtime
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Endpoint: Non-realtime - traffic (source / medium / sessions) ----------
@app.route("/traffic", methods=["GET"])
def traffic_report():
    """
    Query params:
      start_date (default '7daysAgo'), end_date (default 'today'), limit (default 100)
    Returns:
      rows: [{ source, medium, sessions }]
    """
    start_date = request.args.get("start_date", "7daysAgo")
    end_date = request.args.get("end_date", "today")
    limit = int(request.args.get("limit", "100"))
    cache_key = f"traffic:{start_date}:{end_date}:{limit}"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        client = get_ga_client()
        rr = RunReportRequest(
            property=PROPERTY,
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="source"), Dimension(name="medium")],
            metrics=[Metric(name="sessions")],
            limit=limit,
        )
        resp = client.run_report(rr)

        rows = []
        for r in resp.rows:
            src = r.dimension_values[0].value if len(r.dimension_values) > 0 else "(unknown)"
            med = r.dimension_values[1].value if len(r.dimension_values) > 1 else "(unknown)"
            sess = 0
            if r.metric_values:
                try:
                    sess = int(float(r.metric_values[0].value))
                except Exception:
                    sess = 0
            rows.append({"source": src, "medium": med, "sessions": sess})

        out = {"rows": rows, "rowCount": len(rows), "fetchedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        set_cached(cache_key, out, ttl=30)
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Endpoint: Realtime - top countries ----------
@app.route("/top-countries", methods=["GET"])
def top_countries():
    """
    Returns realtime active users grouped by country.
    """
    cache_key = "top-countries"
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        client = get_ga_client()
        request_body = RunRealtimeReportRequest(
            property=PROPERTY,
            dimensions=[Dimension(name="country")],
            metrics=[Metric(name="activeUsers")],
            limit=50,
        )
        resp = client.run_realtime_report(request_body)

        rows = []
        if resp.rows:
            for r in resp.rows:
                country = r.dimension_values[0].value if r.dimension_values else "(unknown)"
                val = 0
                if r.metric_values:
                    try:
                        val = int(float(r.metric_values[0].value))
                    except Exception:
                        val = 0
                rows.append({"country": country, "activeUsers": val})

        out = {"rows": rows, "fetchedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        set_cached(cache_key, out)
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- Simple health ----------
@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "property": PROPERTY})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
