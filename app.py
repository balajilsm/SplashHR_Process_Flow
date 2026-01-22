# app.py
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

try:
    from graphviz import Digraph
    GRAPHVIZ_AVAILABLE = True
except Exception:
    GRAPHVIZ_AVAILABLE = False


# -----------------------------
# App Config
# -----------------------------
st.set_page_config(
    page_title="Process Validation • Pipeline → ETL → Objects → Security",
    page_icon="✅",
    layout="wide"
)

# -----------------------------
# Data Structures
# -----------------------------
@dataclass
class StepResult:
    step_id: str
    step_name: str
    status: str              # PASS / FAIL / WARN / SKIPPED / NOT_RUN
    started_at: str
    ended_at: str
    duration_sec: float
    message: str
    details: str


# -----------------------------
# Helpers
# -----------------------------
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def duration_sec(start_ts: float, end_ts: float) -> float:
    return round(end_ts - start_ts, 3)

def badge(status: str) -> str:
    # Render as emoji + text
    m = {
        "PASS": "✅ PASS",
        "FAIL": "❌ FAIL",
        "WARN": "⚠️ WARN",
        "SKIPPED": "⏭️ SKIPPED",
        "NOT_RUN": "🟦 NOT RUN",
        "RUNNING": "🔄 RUNNING"
    }
    return m.get(status, status)

def init_state():
    if "results" not in st.session_state:
        st.session_state.results: Dict[str, StepResult] = {}
    if "logs" not in st.session_state:
        st.session_state.logs: List[str] = []
    if "last_run" not in st.session_state:
        st.session_state.last_run: Optional[str] = None

def log(line: str):
    st.session_state.logs.insert(0, f"[{now_str()}] {line}")

def to_df(results: Dict[str, StepResult]) -> pd.DataFrame:
    rows = [asdict(v) for v in results.values()]
    if not rows:
        return pd.DataFrame(columns=[
            "step_id","step_name","status","started_at","ended_at","duration_sec","message","details"
        ])
    df = pd.DataFrame(rows)
    # order
    order = ["PIPELINE", "ETL", "OBJECTS", "SECURITY"]
    df["order"] = df["step_id"].apply(lambda x: order.index(x) if x in order else 999)
    df = df.sort_values("order").drop(columns=["order"])
    return df

def overall_status(results: Dict[str, StepResult]) -> str:
    if not results:
        return "NOT_RUN"
    statuses = [r.status for r in results.values()]
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    if any(s == "WARN" for s in statuses):
        return "WARN"
    if all(s == "PASS" for s in statuses) and len(statuses) == 4:
        return "PASS"
    return "WARN"

# -----------------------------
# Mock Validators (Replace these with real checks)
# -----------------------------
def validate_pipeline(simulated_seconds: float, strict_mode: bool) -> Tuple[str, str, str]:
    """
    Replace with:
      - check orchestration job status (Airflow/ADF/Databricks/Prefect)
      - verify last run success + duration threshold
      - verify expected source files landed
    """
    time.sleep(simulated_seconds)

    # deterministic-ish mock using random
    roll = random.random()
    if roll < 0.80:
        return "PASS", "Pipeline completed successfully", "All pipeline stages reported success."
    if roll < 0.93:
        return "WARN", "Pipeline completed with retries", "Some tasks retried but final state is success."
    return ("FAIL", "Pipeline failed",
            "At least one pipeline task is in FAILED state. Check orchestration logs and rerun.")

def validate_etl(simulated_seconds: float, strict_mode: bool) -> Tuple[str, str, str]:
    """
    Replace with:
      - check ETL job exit code
      - validate row counts, load timestamps
      - validate checksum/hash totals
    """
    time.sleep(simulated_seconds)
    roll = random.random()
    if roll < 0.78:
        return "PASS", "ETL transformations succeeded", "Transform and load steps completed."
    if roll < 0.92:
        return "WARN", "ETL succeeded but anomalies detected", "Row count variance exceeded threshold (mock)."
    return "FAIL", "ETL failed", "One or more transformations errored. Review ETL logs."

def validate_objects(simulated_seconds: float, strict_mode: bool, expected_objects: List[str]) -> Tuple[str, str, str]:
    """
    Replace with:
      - check all required tables/views/materialized views exist
      - check last_updated timestamps
      - check schema drift
    """
    time.sleep(simulated_seconds)

    if not expected_objects:
        return "WARN", "No expected objects provided", "Add expected object names in Settings."

    # mock: some objects missing
    missing = []
    for obj in expected_objects:
        if random.random() < 0.08:  # 8% chance missing
            missing.append(obj)

    if not missing:
        return "PASS", "All objects loaded", f"Validated {len(expected_objects)} objects exist."
    if strict_mode:
        return "FAIL", "Missing required objects", "Missing: " + ", ".join(missing)
    return "WARN", "Some objects missing", "Missing: " + ", ".join(missing)

def validate_security(simulated_seconds: float, strict_mode: bool, roles_to_check: List[str]) -> Tuple[str, str, str]:
    """
    Replace with:
      - validate RLS/CLS policies exist
      - validate role-to-object grants
      - validate sample user access tests
    """
    time.sleep(simulated_seconds)

    if not roles_to_check:
        return "WARN", "No roles provided", "Add roles/users to validate in Settings."

    # mock: security drift
    drift = []
    for r in roles_to_check:
        if random.random() < 0.10:  # 10% chance drift
            drift.append(r)

    if not drift:
        return "PASS", "Security checks passed", f"Validated roles: {', '.join(roles_to_check)}"
    if strict_mode:
        return "FAIL", "Security policy mismatch", "Drift detected for: " + ", ".join(drift)
    return "WARN", "Security drift detected", "Drift detected for: " + ", ".join(drift)

# -----------------------------
# Runner
# -----------------------------
def run_step(step_id: str, settings: dict) -> StepResult:
    step_map = {
        "PIPELINE": ("Data Pipeline Process", validate_pipeline),
        "ETL": ("ETL", validate_etl),
        "OBJECTS": ("Loaded All Objects", validate_objects),
        "SECURITY": ("Check Security", validate_security),
    }
    name, fn = step_map[step_id]
    start_wall = time.time()
    started_at = now_str()
    log(f"Starting: {name}")

    # "RUNNING" placeholder
    st.session_state.results[step_id] = StepResult(
        step_id=step_id,
        step_name=name,
        status="RUNNING",
        started_at=started_at,
        ended_at="",
        duration_sec=0.0,
        message="Running...",
        details=""
    )

    # call validator
    try:
        if step_id == "PIPELINE":
            status, msg, details = fn(settings["sim_seconds"], settings["strict_mode"])
        elif step_id == "ETL":
            status, msg, details = fn(settings["sim_seconds"], settings["strict_mode"])
        elif step_id == "OBJECTS":
            status, msg, details = fn(settings["sim_seconds"], settings["strict_mode"], settings["expected_objects"])
        elif step_id == "SECURITY":
            status, msg, details = fn(settings["sim_seconds"], settings["strict_mode"], settings["roles_to_check"])
        else:
            status, msg, details = "SKIPPED", "Unknown step", ""
    except Exception as e:
        status, msg, details = "FAIL", "Validator exception", str(e)

    ended_at = now_str()
    end_wall = time.time()

    result = StepResult(
        step_id=step_id,
        step_name=name,
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        duration_sec=duration_sec(start_wall, end_wall),
        message=msg,
        details=details
    )

    st.session_state.results[step_id] = result
    log(f"Completed: {name} → {status}")
    return result

def run_flow(settings: dict, steps: List[str]) -> None:
    st.session_state.last_run = now_str()
    log(f"Run started (steps={steps})")
    for s in steps:
        # simple dependency gate: stop if FAIL and stop_on_fail enabled
        if settings["stop_on_fail"]:
            if any(r.status == "FAIL" for r in st.session_state.results.values()):
                st.session_state.results[s] = StepResult(
                    step_id=s,
                    step_name={
                        "PIPELINE": "Data Pipeline Process",
                        "ETL": "ETL",
                        "OBJECTS": "Loaded All Objects",
                        "SECURITY": "Check Security"
                    }[s],
                    status="SKIPPED",
                    started_at="",
                    ended_at="",
                    duration_sec=0.0,
                    message="Skipped due to previous failure",
                    details="Enable 'Continue on Fail' to run remaining steps."
                )
                log(f"Skipped: {s} due to previous failure")
                continue

        run_step(s, settings)

    log("Run finished")

# -----------------------------
# UI Components
# -----------------------------
def render_flow_diagram(results: Dict[str, StepResult]):
    st.subheader("Process Flow")
    if not GRAPHVIZ_AVAILABLE:
        st.info("Graphviz not available. Add `graphviz` to requirements.txt to show the flow diagram.")
        return

    def node_color(status: str) -> str:
        # Light colors that work in Streamlit; Graphviz uses color names/hex
        if status == "PASS":
            return "#D1FAE5"  # light green
        if status == "WARN":
            return "#FEF3C7"  # light yellow
        if status == "FAIL":
            return "#FEE2E2"  # light red
        if status == "RUNNING":
            return "#DBEAFE"  # light blue
        if status in ("SKIPPED", "NOT_RUN"):
            return "#E5E7EB"  # gray
        return "#E5E7EB"

    g = Digraph("process", format="png")
    g.attr(rankdir="LR", fontsize="12")
    g.attr("node", shape="box", style="rounded,filled", fontname="Helvetica")

    steps = [
        ("PIPELINE", "Data Pipeline\nProcess"),
        ("ETL", "ETL"),
        ("OBJECTS", "Loaded All\nObjects"),
        ("SECURITY", "Check\nSecurity"),
    ]

    for sid, label in steps:
        status = results.get(sid).status if sid in results else "NOT_RUN"
        g.node(sid, f"{label}\n{badge(status)}", fillcolor=node_color(status))

    g.edge("PIPELINE", "ETL")
    g.edge("ETL", "OBJECTS")
    g.edge("OBJECTS", "SECURITY")

    st.graphviz_chart(g)

def render_summary(results: Dict[str, StepResult]):
    st.subheader("Summary")
    status = overall_status(results)
    cols = st.columns([1, 2, 2, 2])
    cols[0].metric("Overall", badge(status))
    cols[1].metric("Passed", sum(1 for r in results.values() if r.status == "PASS"))
    cols[2].metric("Warnings", sum(1 for r in results.values() if r.status == "WARN"))
    cols[3].metric("Failed", sum(1 for r in results.values() if r.status == "FAIL"))

def render_results_table(results: Dict[str, StepResult]):
    st.subheader("Validation Results")
    df = to_df(results)

    # Make status more readable
    if not df.empty:
        df2 = df.copy()
        df2["status"] = df2["status"].apply(badge)
        st.dataframe(df2, use_container_width=True, hide_index=True)
    else:
        st.info("No results yet. Click **Run All** to start validation.")

def render_logs():
    st.subheader("Logs")
    if st.session_state.logs:
        st.code("\n".join(st.session_state.logs[:250]), language="text")
    else:
        st.info("No logs yet.")

# -----------------------------
# Main
# -----------------------------
init_state()

st.title("✅ Process Validation")
st.caption("Validate your end-to-end flow: Pipeline → ETL → Objects → Security")

with st.sidebar:
    st.header("⚙️ Settings")
    strict_mode = st.toggle("Strict Mode (WARN → FAIL)", value=False)
    stop_on_fail = st.toggle("Stop on Fail", value=True)
    sim_seconds = st.slider("Simulation seconds per step (for demo)", 0.0, 2.0, 0.3, 0.1)

    st.divider()
    st.subheader("Expected Objects")
    default_objs = "HR_EMPLOYEE_F\nHR_JOB_F\nWFA_FACT_HEADCOUNT\nWFA_ATTRITION_F"
    expected_objects_text = st.text_area("One per line", value=default_objs, height=120)

    st.subheader("Roles / Users to Validate")
    default_roles = "HR_ADMIN\nHR_MANAGER\nFIN_ANALYST"
    roles_text = st.text_area("One per line", value=default_roles, height=90)

    st.divider()
    c1, c2 = st.columns(2)
    if c1.button("🧹 Clear Results", use_container_width=True):
        st.session_state.results = {}
        log("Results cleared")
    if c2.button("🧾 Clear Logs", use_container_width=True):
        st.session_state.logs = []
        log("Logs cleared")

settings = {
    "strict_mode": strict_mode,
    "stop_on_fail": stop_on_fail,
    "sim_seconds": sim_seconds,
    "expected_objects": [x.strip() for x in expected_objects_text.splitlines() if x.strip()],
    "roles_to_check": [x.strip() for x in roles_text.splitlines() if x.strip()],
}

top = st.columns([2, 2, 2, 4])

if top[0].button("▶️ Run All", use_container_width=True):
    run_flow(settings, ["PIPELINE", "ETL", "OBJECTS", "SECURITY"])

if top[1].button("▶️ Run Pipeline Only", use_container_width=True):
    run_flow(settings, ["PIPELINE"])

if top[2].button("▶️ Run ETL Only", use_container_width=True):
    run_flow(settings, ["ETL"])

top[3].markdown(
    f"**Last run:** {st.session_state.last_run or '—'}  \n"
    f"**Mode:** {'Strict' if settings['strict_mode'] else 'Normal'} • "
    f"{'Stop on Fail' if settings['stop_on_fail'] else 'Continue on Fail'}"
)

# Layout
left, right = st.columns([1.2, 1])

with left:
    render_flow_diagram(st.session_state.results)
    render_results_table(st.session_state.results)

with right:
    render_summary(st.session_state.results)
    render_logs()

st.divider()
st.markdown(
    """
### 🔌 How to connect real validations
Replace the mock functions:
- `validate_pipeline()` → check your orchestration (ADF/Airflow/Databricks Jobs) status + last run
- `validate_etl()` → check ETL logs, exit code, row counts, timestamps
- `validate_objects()` → query DB for required tables/views & schema checks
- `validate_security()` → validate RLS/CLS rules, grants, and sample user access

If you tell me your stack (ADF/Airflow/Databricks/UKG + Oracle/Postgres), I can plug real checks.
"""
)
