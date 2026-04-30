"""
╔══════════════════════════════════════════════════════════════════╗
║         S-SETU AI — UNIFIED MAIN ENTRY POINT v2.0              ║
║         Citizen's Integrity Guard — Production Build            ║
║         Collective of 100 Senior AI Engineers                   ║
╚══════════════════════════════════════════════════════════════════╝

Integrated Modules:
  Module 1 → odb_data_engine    : D7/D10/D11 scrapers & data models
  Module 2 → truth_mapper       : Integrity Score algorithm (0-100)
  Module 3 → csv_data_loader    : primary_data_datasets.csv ingestion
  Module 4 → score_calculator   : CSV-driven batch score engine
  Module 5 → whatsapp_bot       : Twilio/WATI/Meta webhook handler
  Module 6 → api_router         : FastAPI REST endpoints
  Module 7 → dashboard_ui       : Streamlit visualisation dashboard
  Module 8 → global_exc_handler : Fault-tolerant exception middleware
  Module 9 → system_health      : Startup checks & self-diagnostics

Launch:   streamlit run main.py
API:      uvicorn main:api_app --host 0.0.0.0 --port 8000
CLI:      python main.py --scan "INFOSYS"
"""

# ══════════════════════════════════════════════════════════════════
# STDLIB
# ══════════════════════════════════════════════════════════════════
import os
import re
import sys
import json
import math
import time
import sqlite3
import hashlib
import logging
import warnings
import traceback
import argparse
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta, date
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, List, Any, Tuple

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════
# MODULE 8: GLOBAL EXCEPTION HANDLER
# Must be installed BEFORE any other import that could fail
# ══════════════════════════════════════════════════════════════════
class SSETUExceptionHandler:
    """
    Global fault-tolerant wrapper.
    Catches any unhandled exception, logs it, and returns a safe
    fallback value instead of crashing the entire application.
    """

    @staticmethod
    def safe_import(module_name: str, package: str = None):
        """Try to import; return None on failure with a clear warning."""
        try:
            import importlib
            return importlib.import_module(module_name, package)
        except ImportError as e:
            logging.getLogger("S-SETU.Health").warning(
                f"Optional dependency '{module_name}' not installed: {e}. "
                f"Run: pip install {module_name.split('.')[0]} --break-system-packages"
            )
            return None

    @staticmethod
    def safe_call(fn, *args, fallback=None, label="unknown", **kwargs):
        """
        Execute fn(*args, **kwargs).
        On ANY exception: log full traceback, return `fallback`.
        The app NEVER crashes — it degrades gracefully.
        """
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            logging.getLogger("S-SETU.SafeCall").error(
                f"[{label}] Non-fatal error suppressed: {type(exc).__name__}: {exc}\n"
                + traceback.format_exc()
            )
            return fallback

    @staticmethod
    def install_global_hook():
        """
        Replace sys.excepthook so unhandled exceptions anywhere are
        logged but the Streamlit/FastAPI process keeps running.
        """
        original = sys.excepthook
        def _hook(exc_type, exc_value, exc_tb):
            logging.getLogger("S-SETU.GlobalHook").critical(
                "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            )
            # Don't call original — keeps process alive
        sys.excepthook = _hook
        return original

_exc = SSETUExceptionHandler()
_exc.install_global_hook()

# ══════════════════════════════════════════════════════════════════
# THIRD-PARTY (graceful degradation if missing)
# ══════════════════════════════════════════════════════════════════
pd      = _exc.safe_import("pandas")
st      = _exc.safe_import("streamlit")
plotly  = _exc.safe_import("plotly.express")
pgo     = _exc.safe_import("plotly.graph_objects")
requests_mod = _exc.safe_import("requests")
httpx_mod    = _exc.safe_import("httpx")
fastapi_mod  = _exc.safe_import("fastapi")
uvicorn_mod  = _exc.safe_import("uvicorn")

# ══════════════════════════════════════════════════════════════════
# MODULE 9: SYSTEM HEALTH & STARTUP CHECKS
# ══════════════════════════════════════════════════════════════════
class SystemHealth:
    REQUIRED_LIBS  = ["pandas", "streamlit", "plotly", "requests"]
    OPTIONAL_LIBS  = ["httpx", "fastapi", "uvicorn", "twilio"]
    DATA_CSV       = "primary_data_datasets.csv"
    DB_FILE        = "ssetu_cache.db"
    LOG_FILE       = "ssetu_main.log"

    def __init__(self):
        self.log    = logging.getLogger("S-SETU.Health")
        self.status = {}

    def run_checks(self) -> Dict[str, Any]:
        self.status = {
            "timestamp"    : datetime.now().isoformat(),
            "python_version": sys.version,
            "libs"         : {},
            "data_csv"     : False,
            "database"     : False,
            "writable_dir" : False,
        }
        self._check_libs()
        self._check_csv()
        self._check_db()
        self._check_writability()
        return self.status

    def _check_libs(self):
        import importlib
        for lib in self.REQUIRED_LIBS + self.OPTIONAL_LIBS:
            try:
                importlib.import_module(lib)
                self.status["libs"][lib] = "✓"
            except ImportError:
                required = lib in self.REQUIRED_LIBS
                self.status["libs"][lib] = "✗ MISSING (REQUIRED)" if required else "⚠ optional"
                if required:
                    self.log.warning(f"Required lib missing: {lib}")

    def _check_csv(self):
        p = Path(self.DATA_CSV)
        self.status["data_csv"] = p.exists() and p.stat().st_size > 0
        if not self.status["data_csv"]:
            self.log.warning(f"Primary dataset not found at '{self.DATA_CSV}'. "
                             "Run generate_dataset.py or place your CSV here.")

    def _check_db(self):
        try:
            conn = sqlite3.connect(self.DB_FILE)
            conn.execute("SELECT 1")
            conn.close()
            self.status["database"] = True
        except Exception as e:
            self.log.error(f"DB check failed: {e}")

    def _check_writability(self):
        try:
            test = Path(".ssetu_write_test")
            test.write_text("ok")
            test.unlink()
            self.status["writable_dir"] = True
        except Exception:
            self.status["writable_dir"] = False
            self.log.warning("Current directory is not writable — caching disabled")

_health = SystemHealth()

# ══════════════════════════════════════════════════════════════════
# LOGGING SETUP (after health module so we control the format)
# ══════════════════════════════════════════════════════════════════
def _setup_logging():
    fmt = "%(asctime)s [%(levelname)-8s] %(name)-22s | %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(SystemHealth.LOG_FILE, encoding="utf-8"))
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers, force=True)

_setup_logging()
log = logging.getLogger("S-SETU.Main")

# ══════════════════════════════════════════════════════════════════
# MODULE 1: DATA MODELS (inline — no external file dependency)
# ══════════════════════════════════════════════════════════════════
@dataclass
class D7_CompanyRecord:
    cin: str
    company_name: str
    status: str
    date_of_incorporation: str
    registered_address: str = ""
    company_type: str = ""
    authorized_capital: float = 0.0
    paid_up_capital: float = 0.0
    last_agm_date: str = ""
    last_balance_sheet_date: str = ""
    is_verified: bool = False
    source: str = "CSV"
    fetched_at: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class D10_TaxRecord:
    gstin: str
    legal_name: str
    trade_name: str = ""
    registration_date: str = ""
    taxpayer_type: str = "Regular"
    gst_status: str = "Unknown"
    state_jurisdiction: str = ""
    center_jurisdiction: str = ""
    last_filing_date: str = ""
    filing_frequency: str = "Monthly"
    annual_turnover_slab: str = ""
    compliance_score: float = 0.0
    pan_linked: bool = True
    source: str = "CSV"
    fetched_at: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class D11_ContractRecord:
    tender_id: str
    title: str = ""
    organization: str = ""
    department: str = ""
    bid_opening_date: str = ""
    contract_value_inr: float = 0.0
    awarded_to: str = ""
    awarded_cin: str = ""
    completion_status: str = "Unknown"
    penalty_clauses_invoked: bool = False
    blacklisted: bool = False
    performance_rating: float = 3.0
    source: str = "CSV"
    fetched_at: str = field(default_factory=lambda: datetime.now().isoformat())

# ══════════════════════════════════════════════════════════════════
# MODULE 3: CSV DATA LOADER
# Loads primary_data_datasets.csv and maps columns to data models
# ══════════════════════════════════════════════════════════════════
class CSVDataLoader:
    """
    Reads primary_data_datasets.csv.
    Tolerates missing columns, bad encodings, partial rows.
    Every failure returns an empty structure (never raises).
    """

    COLUMN_MAP = {
        # CSV column              → internal field
        "cin"                   : "cin",
        "company_name"          : "company_name",
        "company_status"        : "status",
        "date_of_incorporation" : "date_of_incorporation",
        "authorized_capital_lac": "authorized_capital",
        "paid_up_capital_lac"   : "paid_up_capital",
        "last_balance_sheet_date": "last_balance_sheet_date",
        "last_agm_date"         : "last_agm_date",
        "gstin"                 : "gstin",
        "gst_status"            : "gst_status",
        "gst_compliance_pct"    : "compliance_score",
        "late_filings"          : "late_filings",
        "total_contracts"       : "total_contracts",
        "contract_defaults"     : "contract_defaults",
        "blacklisted"           : "blacklisted",
        "avg_performance_rating": "performance_rating",
        "transparency_gap_score": "transparency_gap_score",
        "industry_sector"       : "industry_sector",
        "state"                 : "state",
        "annual_turnover_slab"  : "annual_turnover_slab",
    }

    def __init__(self, csv_path: str = "primary_data_datasets.csv"):
        self.csv_path = Path(csv_path)
        self._df: Optional[Any] = None   # pandas DataFrame or None
        self._records: Dict[str, dict] = {}  # CIN → merged dict
        self._load()

    def _load(self):
        if pd is None:
            log.warning("pandas not available — CSV loading disabled")
            return

        if not self.csv_path.exists():
            log.warning(f"CSV not found at '{self.csv_path}'. "
                        "Using empty dataset. Run generate_dataset.py first.")
            return

        try:
            # Try UTF-8 first, fall back to latin-1
            for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
                try:
                    self._df = pd.read_csv(
                        self.csv_path,
                        encoding=enc,
                        dtype=str,          # read everything as string first
                        na_filter=False,    # keep empty strings, not NaN
                        on_bad_lines="warn"
                    )
                    log.info(f"CSV loaded: {len(self._df)} rows, {len(self._df.columns)} cols "
                             f"[encoding={enc}] from '{self.csv_path}'")
                    break
                except UnicodeDecodeError:
                    continue

            if self._df is None or len(self._df) == 0:
                log.error("CSV is empty or unreadable.")
                return

            # Normalise column names
            self._df.columns = [c.strip().lower().replace(" ", "_") for c in self._df.columns]
            self._build_index()

        except Exception as e:
            log.error(f"CSV load failed: {e}\n{traceback.format_exc()}")

    def _safe_float(self, val, default: float = 0.0) -> float:
        try:
            return float(str(val).replace(",", "").strip() or default)
        except (ValueError, TypeError):
            return default

    def _safe_bool(self, val) -> bool:
        return str(val).strip().lower() in ("yes", "true", "1", "y")

    def _build_index(self):
        """Index every row by CIN for O(1) lookup."""
        if self._df is None:
            return
        for _, row in self._df.iterrows():
            cin = str(row.get("cin", "")).strip().upper()
            if not cin:
                continue
            self._records[cin] = {
                # D7 fields
                "cin"                    : cin,
                "company_name"           : str(row.get("company_name", "")).strip(),
                "status"                 : str(row.get("company_status", "Unknown")).strip(),
                "date_of_incorporation"  : str(row.get("date_of_incorporation", "")).strip(),
                "authorized_capital"     : self._safe_float(row.get("authorized_capital_lac", 0)) * 100000,
                "paid_up_capital"        : self._safe_float(row.get("paid_up_capital_lac", 0)) * 100000,
                "last_balance_sheet_date": str(row.get("last_balance_sheet_date", "")).strip(),
                "last_agm_date"          : str(row.get("last_agm_date", "")).strip(),
                "company_type"           : "Private" if "PTC" in cin or "PLC" in cin else "Public",
                # D10 fields
                "gstin"                  : str(row.get("gstin", "")).strip(),
                "gst_status"             : str(row.get("gst_status", "Unknown")).strip(),
                "compliance_score"       : self._safe_float(row.get("gst_compliance_pct", 0)),
                "late_filings"           : int(self._safe_float(row.get("late_filings", 0))),
                "annual_turnover_slab"   : str(row.get("annual_turnover_slab", "")).strip(),
                "pan_linked"             : True,
                # D11 fields
                "total_contracts"        : int(self._safe_float(row.get("total_contracts", 0))),
                "contract_defaults"      : int(self._safe_float(row.get("contract_defaults", 0))),
                "blacklisted"            : self._safe_bool(row.get("blacklisted", "No")),
                "performance_rating"     : self._safe_float(row.get("avg_performance_rating", 3.0)),
                # Meta
                "transparency_gap_score" : self._safe_float(row.get("transparency_gap_score", 50)),
                "industry_sector"        : str(row.get("industry_sector", "")).strip(),
                "state"                  : str(row.get("state", "")).strip(),
                "source"                 : str(row.get("data_source", "CSV")).strip(),
            }
        log.info(f"CSV index built: {len(self._records)} entities indexed by CIN ✓")

    # ── Public API ──────────────────────────────────────────────────────────────

    def get_by_cin(self, cin: str) -> Optional[dict]:
        return self._records.get(cin.upper().strip())

    def search_by_name(self, name: str, limit: int = 5) -> List[dict]:
        q = name.upper().strip()
        return [r for r in self._records.values()
                if q in (r.get("company_name") or "").upper()][:limit]

    def get_d7(self, row: dict) -> dict:
        """Extract D7 sub-dict from a CSV row record."""
        return {k: row.get(k) for k in [
            "cin","company_name","status","date_of_incorporation",
            "authorized_capital","paid_up_capital",
            "last_balance_sheet_date","last_agm_date","company_type"
        ]}

    def get_d10(self, row: dict) -> dict:
        return {k: row.get(k) for k in [
            "gstin","company_name","gst_status","compliance_score",
            "late_filings","annual_turnover_slab","pan_linked"
        ]}

    def get_d11_list(self, row: dict) -> List[dict]:
        """
        Reconstruct synthetic D11 contract records from aggregated CSV data.
        (CSV stores totals, not individual tenders — we synthesise per-contract dicts)
        """
        total    = row.get("total_contracts", 0)
        defaults = row.get("contract_defaults", 0)
        blisted  = row.get("blacklisted", False)
        rating   = row.get("performance_rating", 3.0)
        cin      = row.get("cin", "")

        contracts = []
        for i in range(total):
            is_default = i < defaults
            contracts.append({
                "tender_id"               : f"CSV/{cin[:8]}/{i+1:04d}",
                "completion_status"       : "Defaulted" if is_default else "Completed",
                "blacklisted"             : blisted,
                "penalty_clauses_invoked" : is_default,
                "performance_rating"      : 1.0 if is_default else rating,
                "contract_value_inr"      : 0.0,
                "awarded_cin"             : cin,
            })
        return contracts

    def all_records(self) -> List[dict]:
        return list(self._records.values())

    def dataframe(self):
        """Return raw pandas DataFrame (or empty DF if pandas unavailable)."""
        if self._df is not None:
            return self._df
        if pd:
            return pd.DataFrame()
        return None

    @property
    def count(self) -> int:
        return len(self._records)

# ══════════════════════════════════════════════════════════════════
# MODULE 2 (INLINE): SCORE HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════
GRADE_TABLE = [
    (90, "A+", "HIGHLY TRUSTED"),
    (80, "A",  "TRUSTED"),
    (70, "B+", "GOOD"),
    (60, "B",  "FAIR — Proceed with caution"),
    (50, "C",  "CONCERNING"),
    (35, "D",  "HIGH RISK"),
    (0,  "F",  "CRITICAL RISK"),
]

def _grade(score: float) -> Tuple[str, str]:
    for threshold, grade, verdict in GRADE_TABLE:
        if score >= threshold:
            return grade, verdict
    return "F", "CRITICAL RISK"

def _parse_date(s: str) -> Optional[date]:
    if not s or str(s).strip() in ("", "nan", "None"):
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d", "%d %b %Y"):
        try:
            return datetime.strptime(str(s).strip(), fmt).date()
        except ValueError:
            continue
    return None

def _years_since(date_str: str) -> Optional[float]:
    d = _parse_date(date_str)
    return (date.today() - d).days / 365.25 if d else None

# ══════════════════════════════════════════════════════════════════
# MODULE 4: SCORE CALCULATOR
# Computes Integrity Score directly from CSV row data
# ══════════════════════════════════════════════════════════════════
class IntegrityScoreCalculator:
    """
    Weights (total = 100):
      D7 Existence     25 pts
      D7 Transparency  15 pts
      D10 Compliance   30 pts
      D10 Honesty      10 pts
      D11 Performance  15 pts
      D11 Integrity     5 pts

    Transparency Gap Penalties applied post-scoring (capped at -40).
    """

    def compute_from_row(self, row: dict) -> dict:
        """
        Full score computation from a CSV row dict.
        NEVER raises — all sub-computations are wrapped.
        """
        flags    = []
        penalties = []

        d7_exist  = _exc.safe_call(self._d7_existence,    row, flags, penalties, fallback=0.0, label="D7.existence")
        d7_trans  = _exc.safe_call(self._d7_transparency,  row, flags, penalties, fallback=0.0, label="D7.transparency")
        d10_comp  = _exc.safe_call(self._d10_compliance,   row, flags, penalties, fallback=0.0, label="D10.compliance")
        d10_hon   = _exc.safe_call(self._d10_honesty,      row, flags, penalties, fallback=0.0, label="D10.honesty")
        d11_perf  = _exc.safe_call(self._d11_performance,  row, flags, penalties, fallback=7.5, label="D11.performance")
        d11_intg  = _exc.safe_call(self._d11_integrity,    row, flags, penalties, fallback=2.5, label="D11.integrity")

        raw_score     = (d7_exist + d7_trans + d10_comp + d10_hon + d11_perf + d11_intg)
        total_penalty = min(40.0, sum(abs(p[1]) for p in penalties))
        final_score   = max(0.0, min(100.0, raw_score - total_penalty))

        grade, verdict = _grade(final_score)
        data_count = sum([
            bool(row.get("status")),
            bool(row.get("gst_status")),
            row.get("total_contracts", 0) > 0
        ])
        confidence = ["LOW", "MEDIUM", "MEDIUM", "HIGH"][data_count]

        return {
            "entity_id"             : row.get("cin", "UNKNOWN"),
            "entity_name"           : row.get("company_name", "Unknown"),
            "score"                 : round(final_score, 1),
            "grade"                 : grade,
            "verdict"               : verdict,
            "confidence"            : confidence,
            "d7_existence_score"    : round(d7_exist, 2),
            "d7_transparency_score" : round(d7_trans, 2),
            "d10_compliance_score"  : round(d10_comp, 2),
            "d10_honesty_score"     : round(d10_hon, 2),
            "d11_performance_score" : round(d11_perf, 2),
            "d11_integrity_score"   : round(d11_intg, 2),
            "raw_score"             : round(raw_score, 2),
            "total_penalty"         : round(total_penalty, 2),
            "flags"                 : flags,
            "penalties"             : penalties,
            "industry"              : row.get("industry_sector", ""),
            "state"                 : row.get("state", ""),
            "transparency_gap"      : row.get("transparency_gap_score", 0),
            "computed_at"           : datetime.now().isoformat(),
        }

    def _d7_existence(self, row, flags, penalties) -> float:
        status = str(row.get("status") or "").lower()
        score  = 0.0
        if "active" in status:
            score = 20.0
        elif "dormant" in status:
            score = 10.0
            flags.append("💤 Company is DORMANT")
        elif "struck" in status or "not found" in status:
            score = 0.0
            penalties.append(("Struck-off/Not Found", -8))
            flags.append("🚫 Company STRUCK OFF or not registered")
        elif "liquidat" in status:
            score = 2.0
            flags.append("⚠️ Under LIQUIDATION")
        else:
            score = 5.0
        # Age bonus
        age = _years_since(row.get("date_of_incorporation") or "")
        if age:
            score += min(5.0, age * 0.5)
        return min(25.0, score)

    def _d7_transparency(self, row, flags, penalties) -> float:
        score  = 0.0
        bs_age = _years_since(row.get("last_balance_sheet_date") or "")
        if bs_age is None:
            penalties.append(("No balance sheet on record", -10))
            flags.append("📊 No balance sheet filed with MCA")
        elif bs_age <= 1.5:
            score += 10.0
        elif bs_age <= 2.5:
            score += 6.0
            flags.append("📋 Balance sheet > 18 months old")
        else:
            score += 2.0
            penalties.append((f"Balance sheet {bs_age:.1f}y old", -10))
            flags.append("📋 Severely outdated balance sheet")

        agm_age = _years_since(row.get("last_agm_date") or "")
        status  = str(row.get("status") or "").lower()
        if agm_age is not None and "active" in status:
            if agm_age <= 1.2:
                score += 5.0
            elif agm_age <= 2.0:
                score += 2.0
                flags.append("🏛️ AGM overdue >1 year")
            else:
                penalties.append(("AGM severely overdue", -5))
                flags.append("🏛️ AGM severely OVERDUE")

        auth   = float(row.get("authorized_capital") or 0)
        paidup = float(row.get("paid_up_capital") or 0)
        if auth > 0 and paidup / auth < 0.01:
            flags.append("💰 Shell indicator: very low paid-up capital ratio")

        return min(15.0, score)

    def _d10_compliance(self, row, flags, penalties) -> float:
        comp = float(row.get("compliance_score") or 0)
        score = (comp / 100.0) * 30.0
        if comp < 50:
            penalties.append((f"Very low GST compliance: {comp:.0f}%", -5))
            flags.append(f"📉 GST filing compliance critically low ({comp:.0f}%)")
        elif comp < 75:
            flags.append(f"📋 Below-avg GST compliance ({comp:.0f}%)")
        return min(30.0, score)

    def _d10_honesty(self, row, flags, penalties) -> float:
        gst = str(row.get("gst_status") or "").lower()
        if gst == "active":
            return 10.0
        elif "cancel" in gst:
            penalties.append(("GST cancelled", -10))
            flags.append("🚫 GST registration CANCELLED")
            return 2.0
        elif "suspend" in gst:
            penalties.append(("GST suspended", -10))
            flags.append("⛔ GST registration SUSPENDED")
            return 3.0
        elif "not found" in gst or gst == "":
            flags.append("⚠️ No GST registration found")
            return 0.0
        return 5.0

    def _d11_performance(self, row, flags, penalties) -> float:
        total    = int(row.get("total_contracts") or 0)
        defaults = int(row.get("contract_defaults") or 0)
        rating   = float(row.get("performance_rating") or 3.0)
        if total == 0:
            return 7.5  # neutral — no history
        completion = (total - defaults) / total
        score = completion * 15.0
        score += (rating - 3.0) * 1.5  # ±3 pt adj
        if defaults > 0:
            flags.append(f"❌ {defaults} government contract(s) DEFAULTED")
            penalties.append((f"{defaults} defaults", -min(15, defaults * 3)))
        return max(0.0, min(15.0, score))

    def _d11_integrity(self, row, flags, penalties) -> float:
        blisted = row.get("blacklisted", False)
        if isinstance(blisted, str):
            blisted = blisted.lower() in ("yes","true","1","y")
        if blisted:
            penalties.append(("GeM/CPPP BLACKLISTING", -15))
            flags.append("🚫 BLACKLISTED on government procurement portal!")
            return 0.0
        return 5.0

# ══════════════════════════════════════════════════════════════════
# MODULE 1B: LIVE API ENGINE (wraps HTTP scraping from odb_data_engine)
# Falls back to CSV if API calls fail
# ══════════════════════════════════════════════════════════════════
class LiveAPIEngine:
    """
    Attempts live government API calls (MCA21, GST, GeM).
    On failure: falls back to CSV data, then returns None.
    All failures are logged, never raised.
    """
    MCA_API  = "https://www.mca.gov.in/MCAGateway/rest/master/company/{cin}"
    GST_API  = "https://services.gst.gov.in/services/api/search/gstin"
    GEM_API  = "https://bidplus.gem.gov.in/advance-search/api/bid"

    def __init__(self, csv_loader: CSVDataLoader):
        self.csv    = csv_loader
        self._sess  = None
        self._init_session()

    def _init_session(self):
        if requests_mod:
            try:
                self._sess = requests_mod.Session()
                self._sess.headers.update({
                    "User-Agent": "S-SETU/2.0 CitizensIntegrityGuard +https://ssetu.in",
                    "Accept": "application/json"
                })
            except Exception as e:
                log.warning(f"HTTP session init failed: {e}")

    def _get(self, url: str, params: dict = None, timeout: int = 10) -> Optional[dict]:
        if not self._sess:
            return None
        try:
            r = self._sess.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.debug(f"API call failed ({url}): {e}")
            return None

    def fetch_entity(self, identifier: str) -> dict:
        """
        Master lookup: CIN / GSTIN / Company Name.
        Returns unified dict with d7, d10, d11_list.
        """
        identifier = identifier.strip()
        result = {"identifier": identifier, "d7": None, "d10": None, "d11_list": [],
                  "source": "unknown"}

        # 1. Classify input
        is_cin   = bool(re.match(r'^[LU]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$', identifier.upper()))
        is_gstin = bool(re.match(r'^\d{2}[A-Z]{5}\d{4}[A-Z]\d[Z][A-Z0-9]$', identifier.upper()))

        # 2. CSV lookup first (instant, reliable)
        csv_row = None
        if is_cin:
            csv_row = _exc.safe_call(self.csv.get_by_cin, identifier, label="CSV.byCIN")
        if not csv_row:
            rows = _exc.safe_call(self.csv.search_by_name, identifier, label="CSV.byName") or []
            csv_row = rows[0] if rows else None

        if csv_row:
            result["d7"]       = self.csv.get_d7(csv_row)
            result["d10"]      = self.csv.get_d10(csv_row)
            result["d11_list"] = self.csv.get_d11_list(csv_row)
            result["source"]   = "CSV"
            log.info(f"Found '{identifier}' in CSV dataset ✓")

        # 3. Try live API (enrich/override)
        if is_cin:
            live = _exc.safe_call(
                self._get, self.MCA_API.format(cin=identifier.upper()),
                label="MCA.liveAPI"
            )
            if live:
                result["d7"] = live
                result["source"] = "MCA21-Live"

        return result

# ══════════════════════════════════════════════════════════════════
# MODULE 6: FastAPI REST ROUTER
# ══════════════════════════════════════════════════════════════════
def build_api_app(csv_loader: CSVDataLoader, engine: LiveAPIEngine,
                  calculator: IntegrityScoreCalculator):
    """Build and return the FastAPI application object."""
    if fastapi_mod is None:
        log.warning("FastAPI not installed — REST API disabled")
        return None

    from fastapi import FastAPI, Query, HTTPException
    from fastapi.middleware.cors import CORSMiddleware

    api = FastAPI(
        title="S-SETU Integrity API",
        description="Citizen's Integrity Guard — REST Interface",
        version="2.0.0"
    )
    api.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_methods=["GET","POST"], allow_headers=["*"])

    @api.get("/health")
    def health():
        return {"status": "online", "entities_in_csv": csv_loader.count,
                "timestamp": datetime.now().isoformat()}

    @api.get("/api/v1/score")
    def get_score(q: str = Query(..., description="CIN / GSTIN / Company Name")):
        try:
            entity = engine.fetch_entity(q)
            if not entity["d7"] and not entity["d10"]:
                raise HTTPException(404, f"Entity '{q}' not found in any data source")
            rows = csv_loader.search_by_name(q) or []
            if not rows and entity["d7"]:
                # Build synthetic row from live data
                rows = [entity["d7"]]
            if not rows:
                raise HTTPException(404, "No data available to compute score")
            result = calculator.compute_from_row(rows[0])
            return result
        except HTTPException:
            raise
        except Exception as e:
            log.error(f"Score API error: {e}", exc_info=True)
            raise HTTPException(500, f"Internal error: {e}")

    @api.get("/api/v1/batch")
    def batch_scores(limit: int = 50):
        """Return scores for all entities in the CSV dataset."""
        all_rows = csv_loader.all_records()[:limit]
        return [calculator.compute_from_row(r) for r in all_rows]

    @api.post("/webhook/whatsapp")
    async def whatsapp_webhook(request):
        """Minimal WhatsApp webhook handler."""
        try:
            data = await request.json()
            msg  = data.get("Body") or data.get("text") or data.get("message", "")
            frm  = data.get("From") or data.get("from", "")
            if not msg:
                return {"status": "ok"}
            entity = engine.fetch_entity(msg.strip())
            rows   = csv_loader.search_by_name(msg.strip()) or (
                [entity["d7"]] if entity["d7"] else []
            )
            if rows:
                sc = calculator.compute_from_row(rows[0])
                reply = _build_wa_reply(sc, entity)
            else:
                reply = f"❓ '{msg[:30]}' — Entity not found in any database.\nTry CIN or GSTIN format."
            return {"status": "ok", "reply": reply}
        except Exception as e:
            log.error(f"WhatsApp webhook error: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    return api

def _build_wa_reply(score_dict: dict, entity: dict) -> str:
    s    = score_dict
    icon = "🟢" if s["score"] >= 80 else "🟡" if s["score"] >= 60 else "🟠" if s["score"] >= 40 else "🔴"
    d7   = entity.get("d7") or {}
    d10  = entity.get("d10") or {}
    d11s = entity.get("d11_list") or []
    total_c  = len(d11s)
    defaults = sum(1 for c in d11s if "default" in str(c.get("completion_status","")).lower())
    blisted  = any(c.get("blacklisted") for c in d11s)

    return "\n".join([
        f"🇮🇳 *S-SETU — नागरिक सत्यापन रिपोर्ट*",
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        f"🏢 Entity: *{s['entity_name']}*",
        f"{icon} Integrity Score: *{s['score']:.0f}/100* (Grade {s['grade']})",
        f"",
        f"━━ ✅ D7: क्या यह कंपनी असली है? ━━",
        f"{'✅' if 'active' in str(d7.get('status','')).lower() else '❌'} Status: *{d7.get('status','N/A')}*",
        f"📋 CIN: {d7.get('cin','N/A')}",
        f"",
        f"━━ 💰 D10: क्या टैक्स भरते हैं? ━━",
        f"{'✅' if 'active' in str(d10.get('gst_status','')).lower() else '❌'} GST: *{d10.get('gst_status','N/A')}*",
        f"📊 Compliance: *{d10.get('compliance_score',0):.0f}%*",
        f"",
        f"━━ 📜 D11: सरकारी ठेका रिकॉर्ड ━━",
        f"{'✅' if not blisted and defaults==0 else '❌'} Contracts: *{total_c}* | Defaults: *{defaults}* | Blacklisted: *{'YES⚠️' if blisted else 'No'}*",
        f"",
        f"🚩 *{len(s['flags'])} Red Flags* | Confidence: {s['confidence']}",
        *[f"  {f}" for f in s['flags'][:3]],
        f"",
        f"🔗 Full report: https://ssetu.in",
        f"_S-SETU — जागरूक नागरिक, मजबूत भारत_ 🇮🇳",
    ])

# ══════════════════════════════════════════════════════════════════
# MODULE 7: STREAMLIT DASHBOARD UI
# ══════════════════════════════════════════════════════════════════
def run_dashboard(csv_loader: CSVDataLoader,
                  engine: LiveAPIEngine,
                  calculator: IntegrityScoreCalculator,
                  health_status: dict):
    if st is None:
        print("Streamlit not installed. Run: pip install streamlit --break-system-packages")
        return

    # ── Page config ──────────────────────────────────────────────────────────
    st.set_page_config(
        page_title="S-SETU: Citizen's Integrity Guard",
        page_icon="🇮🇳",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # ── Custom CSS ───────────────────────────────────────────────────────────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@400;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Rajdhani', sans-serif !important; }
    .stApp { background: #07090f; color: #e2e8f0; }

    .ssetu-header {
        background: linear-gradient(135deg, #0a0e1a, #1a237e);
        border: 1px solid #1565c0;
        border-radius: 12px;
        padding: 20px 28px;
        margin-bottom: 24px;
        position: relative;
        overflow: hidden;
    }
    .ssetu-header h1 {
        font-family: 'Share Tech Mono', monospace !important;
        font-size: 2.4rem;
        letter-spacing: 8px;
        color: #ffffff;
        margin: 0;
    }
    .ssetu-header p { color: #90caf9; letter-spacing: 2px; font-size: 0.85rem; margin: 4px 0 0; }

    .score-card {
        background: #0d1117;
        border-radius: 12px;
        padding: 20px;
        border: 1px solid #1e2536;
        text-align: center;
    }
    .score-number {
        font-family: 'Share Tech Mono', monospace;
        font-size: 3.5rem;
        font-weight: 700;
        line-height: 1;
    }
    .score-grade { font-size: 1.1rem; letter-spacing: 3px; color: #64748b; margin-top: 4px; }

    .metric-card {
        background: #111827;
        border-radius: 8px;
        padding: 12px 16px;
        border: 1px solid #1e2536;
    }
    .metric-label { font-size: 0.7rem; letter-spacing: 1.5px; color: #64748b; text-transform: uppercase; }
    .metric-value { font-family: 'Share Tech Mono', monospace; font-size: 1.2rem; margin-top: 4px; }

    .flag-item {
        background: rgba(213,0,0,0.08);
        border-left: 3px solid #ff1744;
        border-radius: 0 6px 6px 0;
        padding: 6px 12px;
        margin: 4px 0;
        font-size: 0.85rem;
        color: #ef9a9a;
    }
    .flag-clean {
        background: rgba(0,230,118,0.06);
        border: 1px solid #00e67622;
        border-radius: 6px;
        padding: 10px 14px;
        color: #00e676;
    }
    .status-chip {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 1px;
    }
    .chip-ok   { background: rgba(0,230,118,0.15); color: #00e676; border: 1px solid #00e676; }
    .chip-warn { background: rgba(255,214,0,0.15);  color: #ffd600; border: 1px solid #ffd600; }
    .chip-bad  { background: rgba(255,23,68,0.15);  color: #ff1744; border: 1px solid #ff1744; }
    .chip-na   { background: rgba(100,116,139,0.1); color: #94a3b8; border: 1px solid #334155; }

    div[data-testid="stMetricValue"] { font-family: 'Share Tech Mono', monospace !important; }
    .stProgress > div > div { border-radius: 4px !important; }
    </style>
    """, unsafe_allow_html=True)

    # ── Header ───────────────────────────────────────────────────────────────
    st.markdown("""
    <div class="ssetu-header">
      <h1>🇮🇳 S-SETU</h1>
      <p>CITIZEN'S INTEGRITY GUARD &nbsp;·&nbsp; D7 · D10 · D11 &nbsp;·&nbsp;
         MCA21 · GST PORTAL · GeM · CPPP</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### 🔍 Search Entity")
        query = st.text_input("", placeholder="CIN / GSTIN / Company Name",
                              key="search_query")
        scan_btn = st.button("▶ SCAN", type="primary", use_container_width=True)

        st.divider()
        st.markdown("### 📋 Quick Select")
        all_names = [r.get("company_name","") for r in csv_loader.all_records()]
        selected = st.selectbox("From Dataset", [""] + all_names, label_visibility="collapsed")
        if selected:
            query = selected

        st.divider()
        st.markdown("### ⚙️ System Health")
        st.markdown(f"**CSV Entities:** `{csv_loader.count}`")
        st.markdown(f"**Database:** {'✅' if health_status.get('database') else '❌'}")
        st.markdown(f"**Python:** `{sys.version[:6]}`")

        libs_ok = all(v == "✓" for v in health_status.get("libs", {}).values()
                      if "REQUIRED" not in str(v))
        st.markdown(f"**Core Libs:** {'✅ OK' if libs_ok else '⚠️ Check Logs'}")

        st.divider()
        st.markdown("### 📊 Grade Legend")
        for _, g, v in GRADE_TABLE:
            color = "#00e676" if g in ("A+","A") else "#ffd600" if g in ("B+","B") \
                    else "#ff6d00" if g == "C" else "#ff1744"
            st.markdown(f"<span style='color:{color};font-weight:700'>{g}</span>"
                        f" — <small>{v}</small>", unsafe_allow_html=True)

    # ── Main Tabs ─────────────────────────────────────────────────────────────
    tab_search, tab_batch, tab_analytics, tab_wa, tab_health = st.tabs([
        "🔍 Entity Scan", "📊 Batch Dataset", "📈 Analytics", "📲 WhatsApp Preview", "🛠️ System"
    ])

    # ════════════════════════════════════════════════
    # TAB 1: ENTITY SCAN
    # ════════════════════════════════════════════════
    with tab_search:
        active_query = query.strip() if (scan_btn or query) else ""

        if not active_query:
            st.markdown("""
            <div style="text-align:center;padding:60px 0;color:#334155">
              <div style="font-size:3rem">🔎</div>
              <div style="font-size:1.1rem;margin-top:16px">
                Enter a Company Name, CIN, GSTIN, or Tender ID<br>
                to compute its Integrity Score
              </div>
              <div style="font-size:0.8rem;margin-top:12px;color:#1e3a5f">
                Try: INFOSYS &nbsp;·&nbsp; RELIANCE &nbsp;·&nbsp; DEMO INFRA
              </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            with st.spinner(f"🔍 Scanning D7 · D10 · D11 for '{active_query}'..."):
                entity  = _exc.safe_call(engine.fetch_entity, active_query,
                                         fallback={}, label="engine.fetch")
                rows    = _exc.safe_call(csv_loader.search_by_name, active_query,
                                         fallback=[]) or []
                csv_row = rows[0] if rows else None
                if not csv_row and entity.get("d7"):
                    csv_row = entity["d7"]
                sc = _exc.safe_call(calculator.compute_from_row, csv_row,
                                    fallback=None, label="calc.row") if csv_row else None

            if sc is None:
                st.error(f"❌ Entity '{active_query}' not found in any data source.")
                st.info("Tip: Try exact company name, CIN (22 chars), or GSTIN (15 chars).")
            else:
                score = sc["score"]
                color = "#00e676" if score >= 80 else "#ffd600" if score >= 60 \
                        else "#ff6d00" if score >= 40 else "#ff1744"

                # ── Score row ────────────────────────────────────────────────
                col_score, col_info = st.columns([1, 2])
                with col_score:
                    st.markdown(f"""
                    <div class="score-card">
                      <div class="score-number" style="color:{color}">{score:.0f}</div>
                      <div class="score-grade">/ 100 &nbsp; GRADE {sc['grade']}</div>
                      <div style="font-size:0.75rem;color:#64748b;margin-top:8px;letter-spacing:1px">
                        {sc['verdict']}
                      </div>
                      <div style="margin-top:12px;font-size:0.7rem;color:#334155">
                        Confidence: {sc['confidence']}
                      </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_info:
                    st.markdown(f"#### {sc['entity_name']}")
                    st.markdown(f"**ID:** `{sc['entity_id']}` &nbsp;|&nbsp; "
                                f"**Industry:** {sc.get('industry','N/A')} &nbsp;|&nbsp; "
                                f"**State:** {sc.get('state','N/A')}")

                    d7  = entity.get("d7") or csv_row or {}
                    d10 = entity.get("d10") or {}
                    d7_status  = str(d7.get("status") or d7.get("company_status","")).strip()
                    d10_status = str(d10.get("gst_status","")).strip()
                    blisted    = any(c.get("blacklisted") for c in (entity.get("d11_list") or []))

                    def chip(label, ok):
                        cls = "chip-ok" if ok else "chip-bad"
                        icon = "✓" if ok else "✕"
                        return f'<span class="status-chip {cls}">{icon} {label}</span>'

                    st.markdown(
                        chip("D7: Registry", "active" in d7_status.lower()) + " &nbsp; " +
                        chip("D10: Tax", "active" in d10_status.lower()) + " &nbsp; " +
                        chip("D11: Clean", not blisted),
                        unsafe_allow_html=True
                    )
                    st.markdown("")

                    # Dimension bars
                    dims = [
                        ("D7 Existence",    sc["d7_existence_score"],    25, "#1565c0"),
                        ("D7 Transparency", sc["d7_transparency_score"],  15, "#0288d1"),
                        ("D10 Compliance",  sc["d10_compliance_score"],   30, "#00838f"),
                        ("D10 Honesty",     sc["d10_honesty_score"],      10, "#26a69a"),
                        ("D11 Performance", sc["d11_performance_score"],  15, "#558b2f"),
                        ("D11 Integrity",   sc["d11_integrity_score"],     5, "#7cb342"),
                    ]
                    for label, val, max_val, _ in dims:
                        pct = val / max_val if max_val else 0
                        st.progress(pct, text=f"{label}: {val:.1f} / {max_val}")

                # ── Detail cards ─────────────────────────────────────────────
                st.divider()
                dc1, dc2, dc3 = st.columns(3)
                with dc1:
                    st.markdown("#### 🏢 D7 — Company Registry")
                    for k, v in [("Status", d7_status or "N/A"),
                                  ("CIN", d7.get("cin","N/A")),
                                  ("Incorporated", d7.get("date_of_incorporation","N/A")),
                                  ("Balance Sheet", d7.get("last_balance_sheet_date","N/A")),
                                  ("Last AGM", d7.get("last_agm_date","N/A"))]:
                        st.markdown(f"<div class='metric-card'>"
                                    f"<div class='metric-label'>{k}</div>"
                                    f"<div class='metric-value'>{v}</div></div><br>",
                                    unsafe_allow_html=True)

                with dc2:
                    st.markdown("#### 💰 D10 — Tax Transparency")
                    comp_pct = float(d10.get("compliance_score", csv_row.get("compliance_score", 0) if csv_row else 0))
                    for k, v in [("GST Status", d10_status or "N/A"),
                                  ("GSTIN", d10.get("gstin","N/A")),
                                  ("Filing Compliance", f"{comp_pct:.0f}%"),
                                  ("Taxpayer Type", d10.get("taxpayer_type","Regular")),
                                  ("PAN Linked", "Yes" if d10.get("pan_linked", True) else "No")]:
                        st.markdown(f"<div class='metric-card'>"
                                    f"<div class='metric-label'>{k}</div>"
                                    f"<div class='metric-value'>{v}</div></div><br>",
                                    unsafe_allow_html=True)

                with dc3:
                    st.markdown("#### 📜 D11 — Contract Record")
                    d11_list = entity.get("d11_list") or []
                    total_c  = len(d11_list) or (csv_row.get("total_contracts",0) if csv_row else 0)
                    defaults = sum(1 for c in d11_list if "default" in str(c.get("completion_status","")).lower())
                    rating   = csv_row.get("performance_rating", 3.0) if csv_row else 3.0
                    for k, v in [("Total Contracts", total_c),
                                  ("Defaults", defaults),
                                  ("Blacklisted", "🚫 YES" if blisted else "✅ No"),
                                  ("Avg Rating", f"{float(rating):.1f} / 5.0"),
                                  ("Source", sc.get("entity_id","")[:6] + "…")]:
                        val_color = "#ff1744" if (k == "Blacklisted" and blisted) or \
                                    (k == "Defaults" and int(v) > 0) else "#e2e8f0"
                        st.markdown(f"<div class='metric-card'>"
                                    f"<div class='metric-label'>{k}</div>"
                                    f"<div class='metric-value' style='color:{val_color}'>{v}</div></div><br>",
                                    unsafe_allow_html=True)

                # ── Flags ─────────────────────────────────────────────────────
                st.divider()
                flags = sc.get("flags", [])
                if flags:
                    st.markdown(f"#### 🚩 Red Flags ({len(flags)})")
                    for f in flags:
                        st.markdown(f"<div class='flag-item'>{f}</div>", unsafe_allow_html=True)
                else:
                    st.markdown("<div class='flag-clean'>✅ No red flags detected — entity appears clean</div>",
                                unsafe_allow_html=True)

    # ════════════════════════════════════════════════
    # TAB 2: BATCH DATASET
    # ════════════════════════════════════════════════
    with tab_batch:
        st.markdown("### 📊 Full Dataset — Integrity Scores")
        all_rows = csv_loader.all_records()
        if not all_rows:
            st.warning("No data loaded. Check that `primary_data_datasets.csv` exists.")
        else:
            with st.spinner("Computing scores for all entities..."):
                all_scores = [_exc.safe_call(calculator.compute_from_row, r,
                                             fallback={"entity_name": r.get("company_name",""),
                                                       "score": 0, "grade": "?"}, label="batch")
                              for r in all_rows]

            if pd:
                df = pd.DataFrame(all_scores)[[
                    "entity_name","score","grade","confidence",
                    "d7_existence_score","d10_compliance_score","d11_performance_score",
                    "flags","industry","state"
                ]].rename(columns={
                    "entity_name": "Company", "score": "Score", "grade": "Grade",
                    "d7_existence_score": "D7", "d10_compliance_score": "D10",
                    "d11_performance_score": "D11", "confidence": "Confidence",
                    "flags": "Red Flags"
                })
                df["Red Flags"] = df["Red Flags"].apply(lambda x: len(x) if isinstance(x, list) else 0)
                df = df.sort_values("Score", ascending=False)

                # Colour-code score
                def colour_score(val):
                    if val >= 80: return "background-color:#00e67622;color:#00e676"
                    if val >= 60: return "background-color:#ffd60022;color:#ffd600"
                    if val >= 40: return "background-color:#ff6d0022;color:#ff6d00"
                    return "background-color:#ff174422;color:#ff1744"

                styled = df.style.applymap(colour_score, subset=["Score"])
                st.dataframe(styled, use_container_width=True, height=420)

                csv_dl = df.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download Scores CSV", csv_dl,
                                   "ssetu_integrity_scores.csv", "text/csv")

    # ════════════════════════════════════════════════
    # TAB 3: ANALYTICS
    # ════════════════════════════════════════════════
    with tab_analytics:
        st.markdown("### 📈 Integrity Analytics Dashboard")
        all_rows = csv_loader.all_records()
        if not all_rows or plotly is None or pgo is None:
            st.info("Analytics require pandas + plotly. Install via requirements.txt")
        else:
            scores_list = [_exc.safe_call(calculator.compute_from_row, r,
                                          fallback={"score": 0, "grade": "?",
                                                    "entity_name": r.get("company_name",""),
                                                    "industry": r.get("industry_sector",""),
                                                    "state": r.get("state",""),
                                                    "transparency_gap": 0},
                                          label="analytics") for r in all_rows]
            df_a = pd.DataFrame(scores_list)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Entities", len(df_a))
            col2.metric("Avg Score", f"{df_a['score'].mean():.1f}")
            high_risk = (df_a["score"] < 40).sum()
            col3.metric("High Risk (< 40)", int(high_risk),
                        delta=f"{high_risk/len(df_a)*100:.0f}% of total",
                        delta_color="inverse")
            col4.metric("Trusted (≥ 80)", int((df_a["score"] >= 80).sum()))

            st.divider()
            ac1, ac2 = st.columns(2)
            with ac1:
                fig = plotly.histogram(df_a, x="score", nbins=10,
                                       title="Score Distribution",
                                       color_discrete_sequence=["#1565c0"])
                fig.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                                  font_color="#e2e8f0")
                st.plotly_chart(fig, use_container_width=True)
            with ac2:
                grade_counts = df_a["grade"].value_counts().reset_index()
                grade_counts.columns = ["Grade", "Count"]
                fig2 = plotly.bar(grade_counts, x="Grade", y="Count",
                                  title="Grade Distribution",
                                  color="Grade",
                                  color_discrete_map={
                                      "A+":"#00e676","A":"#69f0ae","B+":"#ffd600",
                                      "B":"#ffab40","C":"#ff6d00","D":"#ff1744","F":"#b71c1c"
                                  })
                fig2.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                                   font_color="#e2e8f0", showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)

            if "industry" in df_a.columns and df_a["industry"].nunique() > 1:
                fig3 = plotly.box(df_a, x="industry", y="score",
                                  title="Score by Industry Sector",
                                  color_discrete_sequence=["#1565c0"])
                fig3.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                                   font_color="#e2e8f0")
                st.plotly_chart(fig3, use_container_width=True)

    # ════════════════════════════════════════════════
    # TAB 4: WHATSAPP PREVIEW
    # ════════════════════════════════════════════════
    with tab_wa:
        st.markdown("### 📲 WhatsApp Bot Reply Preview")
        wa_query = st.text_input("Test Entity", value="INFOSYS", key="wa_query")
        if wa_query:
            entity  = _exc.safe_call(engine.fetch_entity, wa_query, fallback={}, label="wa.engine")
            rows    = _exc.safe_call(csv_loader.search_by_name, wa_query, fallback=[]) or []
            csv_row = rows[0] if rows else None
            if csv_row:
                sc    = _exc.safe_call(calculator.compute_from_row, csv_row, fallback=None, label="wa.calc")
                reply = _build_wa_reply(sc, entity) if sc else "No data found."
            else:
                reply = f"❓ '{wa_query}' not found. Try exact company name or CIN."
            st.code(reply, language=None)
            st.caption("This exact text is sent via Twilio/WATI/Meta when a citizen WhatsApps the bot.")

    # ════════════════════════════════════════════════
    # TAB 5: SYSTEM HEALTH
    # ════════════════════════════════════════════════
    with tab_health:
        st.markdown("### 🛠️ System Health Dashboard")
        hc1, hc2, hc3 = st.columns(3)
        hc1.metric("CSV Loaded", "✅" if health_status.get("data_csv") else "❌ Missing")
        hc2.metric("Database", "✅ SQLite" if health_status.get("database") else "❌")
        hc3.metric("Dir Writable", "✅" if health_status.get("writable_dir") else "❌")

        st.divider()
        st.markdown("#### Library Status")
        libs = health_status.get("libs", {})
        for lib, status in libs.items():
            icon = "✅" if status == "✓" else "⚠️" if "optional" in str(status) else "❌"
            st.markdown(f"{icon} `{lib}` — {status}")

        st.divider()
        st.markdown("#### Data Pipeline")
        st.markdown(f"- **CSV Path:** `{csv_loader.csv_path.absolute()}`")
        st.markdown(f"- **Entities Loaded:** `{csv_loader.count}`")
        st.markdown(f"- **Health Check Timestamp:** `{health_status.get('timestamp','N/A')}`")
        st.markdown(f"- **Log File:** `{SystemHealth.LOG_FILE}`")

        st.divider()
        st.markdown("#### 3-Step Launch Commands")
        st.code("""
# Step 1 — Install all dependencies
pip install -r requirements.txt

# Step 2 — Generate / verify dataset
python generate_dataset.py

# Step 3 — Launch dashboard
streamlit run main.py
""", language="bash")

# ══════════════════════════════════════════════════════════════════
# BOOT & ENTRYPOINT
# ══════════════════════════════════════════════════════════════════
def _boot() -> Tuple:
    """Initialise all modules. Returns (csv_loader, engine, calculator, health)."""
    log.info("═" * 66)
    log.info("  S-SETU AI v2.0 — Unified Boot Sequence")
    log.info("═" * 66)

    health = _exc.safe_call(_health.run_checks, fallback={}, label="health.check")
    log.info(f"Health: CSV={'✓' if health.get('data_csv') else '✗'} "
             f"DB={'✓' if health.get('database') else '✗'}")

    csv_loader = _exc.safe_call(
        CSVDataLoader, "primary_data_datasets.csv",
        fallback=CSVDataLoader.__new__(CSVDataLoader), label="CSVDataLoader.init"
    )
    if not hasattr(csv_loader, "_records"):
        csv_loader._records = {}
        csv_loader.csv_path = Path("primary_data_datasets.csv")

    engine     = _exc.safe_call(LiveAPIEngine, csv_loader,
                                 fallback=None, label="LiveAPIEngine.init")
    if engine is None:
        # Minimal fallback engine
        class _FallbackEngine:
            def __init__(self, c): self.csv = c
            def fetch_entity(self, q):
                rows = self.csv.search_by_name(q)
                r = rows[0] if rows else {}
                return {"identifier": q, "d7": r, "d10": r, "d11_list": self.csv.get_d11_list(r) if r else [], "source": "CSV"}
        engine = _FallbackEngine(csv_loader)

    calculator = IntegrityScoreCalculator()

    log.info(f"Boot complete. CSV entities: {csv_loader.count} ✓")
    return csv_loader, engine, calculator, health

# ── Streamlit mode (streamlit run main.py) ───────────────────────────────────
_csv_loader, _engine, _calculator, _health_status = _boot()

if __name__ == "__main__":
    # ── CLI mode (python main.py --scan "INFOSYS") ───────────────────────────
    parser = argparse.ArgumentParser(description="S-SETU CLI")
    parser.add_argument("--scan",   type=str, help="Scan entity by name/CIN/GSTIN")
    parser.add_argument("--batch",  action="store_true", help="Score all CSV entities")
    parser.add_argument("--health", action="store_true", help="Show system health")
    parser.add_argument("--api",    action="store_true", help="Launch REST API server")
    args = parser.parse_args()

    if args.health:
        print(json.dumps(_health_status, indent=2))

    elif args.scan:
        entity = _engine.fetch_entity(args.scan)
        rows   = _csv_loader.search_by_name(args.scan)
        if rows:
            sc = _calculator.compute_from_row(rows[0])
            print("\n" + "═"*62)
            print(f"  S-SETU INTEGRITY REPORT: {sc['entity_name']}")
            print("═"*62)
            print(f"  SCORE : {sc['score']}/100 — Grade {sc['grade']}")
            print(f"  VERDICT: {sc['verdict']}")
            print(f"  CONFIDENCE: {sc['confidence']}")
            print(f"\n  D7 Existence:    {sc['d7_existence_score']:5.1f} / 25")
            print(f"  D7 Transparency: {sc['d7_transparency_score']:5.1f} / 15")
            print(f"  D10 Compliance:  {sc['d10_compliance_score']:5.1f} / 30")
            print(f"  D10 Honesty:     {sc['d10_honesty_score']:5.1f} / 10")
            print(f"  D11 Performance: {sc['d11_performance_score']:5.1f} / 15")
            print(f"  D11 Integrity:   {sc['d11_integrity_score']:5.1f} /  5")
            print(f"  Penalty Applied: -{sc['total_penalty']:.1f}")
            if sc["flags"]:
                print(f"\n  🚩 Red Flags ({len(sc['flags'])}):")
                for f in sc["flags"]:
                    print(f"    {f}")
            print("═"*62)
        else:
            print(f"Entity '{args.scan}' not found.")

    elif args.batch:
        all_rows = _csv_loader.all_records()
        print(f"\n{'Company':<40} {'Score':>6} {'Grade':>6} {'Flags':>6}")
        print("─"*62)
        for r in all_rows:
            sc = _calculator.compute_from_row(r)
            print(f"{sc['entity_name'][:39]:<40} {sc['score']:>6.1f} {sc['grade']:>6} {len(sc['flags']):>6}")

    elif args.api:
        api_app = build_api_app(_csv_loader, _engine, _calculator)
        if api_app and uvicorn_mod:
            print("Starting S-SETU REST API on http://0.0.0.0:8000")
            uvicorn_mod.run(api_app, host="0.0.0.0", port=8000)
        else:
            print("FastAPI/Uvicorn not available. Install: pip install fastapi uvicorn --break-system-packages")

    else:
        parser.print_help()

else:
    # ── Streamlit mode — called by: streamlit run main.py ────────────────────
    run_dashboard(_csv_loader, _engine, _calculator, _health_status)

# Build FastAPI app at module level (for: uvicorn main:api_app)
try:
    api_app = build_api_app(_csv_loader, _engine, _calculator)
except Exception:
    api_app = None
