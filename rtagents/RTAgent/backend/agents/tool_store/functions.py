"""
functions.py

This module provides asynchronous backend functions for a simulated medical agent system.
It includes user authentication, appointment scheduling, prescription management, medication lookup,
side effect and drug interaction checks, and emergency escalation. The module uses in-memory
dictionaries to simulate patient, prescription, and medication databases, and returns results
in JSON format for integration with frontend or API layers.
"""

import json
from datetime import date as _date
from datetime import timedelta as _timedelta
from typing import Any, Dict, List, TypedDict

from utils.ml_logging import get_logger

logger = get_logger()

# ------------------------------------------
# Simulated Internal Data ("Databases")
# ------------------------------------------

# Patients and their basic information
patients_db: Dict[str, Dict[str, str]] = {
    "Alice Brown": {"dob": "1987-04-12", "patient_id": "P54321", "phone": "5552971078"},
    "Bob Johnson": {"dob": "1992-11-25", "patient_id": "P98765", "phone": "5558484555"},
    "Charlie Davis": {
        "dob": "1980-01-15",
        "patient_id": "P11223",
        "phone": "5559890662",
    },
    "Diana Evans": {"dob": "1995-07-08", "patient_id": "P33445", "phone": "5554608513"},
    "Ethan Foster": {
        "dob": "1983-03-22",
        "patient_id": "P55667",
        "phone": "5558771166",
    },
    "Fiona Green": {"dob": "1998-09-10", "patient_id": "P77889", "phone": "5557489234"},
    "George Harris": {
        "dob": "1975-12-05",
        "patient_id": "P99001",
        "phone": "5558649200",
    },
    "Hannah Irving": {
        "dob": "1989-06-30",
        "patient_id": "P22334",
        "phone": "5554797595",
    },
    "Ian Jackson": {"dob": "1993-02-18", "patient_id": "P44556", "phone": "5551374879"},
    "Julia King": {"dob": "1986-08-14", "patient_id": "P66778", "phone": "5559643430"},
}

# Patient medications and refill info
prescriptions_db: Dict[str, Dict[str, Dict[str, str]]] = {
    "Alice Brown": {
        "Metformin": {"last_refill": "2024-03-01", "pharmacy": "City Pharmacy"}
    },
    "Bob Johnson": {
        "Atorvastatin": {"last_refill": "2024-02-20", "pharmacy": "Town Pharmacy"}
    },
    "Charlie Davis": {
        "Lisinopril": {"last_refill": "2024-01-15", "pharmacy": "Central Pharmacy"}
    },
    "Diana Evans": {
        "Omeprazole": {"last_refill": "2024-03-05", "pharmacy": "East Pharmacy"}
    },
    "Ethan Foster": {
        "Amlodipine": {"last_refill": "2024-02-28", "pharmacy": "West Pharmacy"}
    },
    "Fiona Green": {
        "Levothyroxine": {"last_refill": "2024-03-10", "pharmacy": "North Pharmacy"}
    },
    "George Harris": {
        "Simvastatin": {"last_refill": "2024-01-25", "pharmacy": "South Pharmacy"}
    },
    "Hannah Irving": {
        "Losartan": {"last_refill": "2024-02-15", "pharmacy": "Downtown Pharmacy"}
    },
    "Ian Jackson": {
        "Hydrochlorothiazide": {
            "last_refill": "2024-03-12",
            "pharmacy": "Uptown Pharmacy",
        }
    },
    "Julia King": {
        "Gabapentin": {"last_refill": "2024-03-08", "pharmacy": "Suburban Pharmacy"}
    },
}

# Medication information
medications_info_db: Dict[str, str] = {
    "Metformin": "Metformin is used to treat type 2 diabetes. Common side effects include nausea and diarrhea.",
    "Atorvastatin": "Atorvastatin is used to lower cholesterol. Side effects may include muscle pain and digestive issues.",
    "Lisinopril": "Lisinopril is used to treat high blood pressure. Side effects may include dizziness and dry cough.",
    "Omeprazole": "Omeprazole is used to treat acid reflux. Side effects may include headache and abdominal pain.",
    "Amlodipine": "Amlodipine is used to treat high blood pressure. Side effects may include swelling and fatigue.",
    "Levothyroxine": "Levothyroxine is used to treat hypothyroidism. Side effects may include weight loss and heat sensitivity.",
    "Simvastatin": "Simvastatin is used to lower cholesterol. Side effects may include muscle pain and liver issues.",
    "Losartan": "Losartan is used to treat high blood pressure. Side effects may include dizziness and back pain.",
    "Hydrochlorothiazide": "Hydrochlorothiazide is used to treat fluid retention. Side effects may include increased urination and dizziness.",
    "Gabapentin": "Gabapentin is used to treat nerve pain. Side effects may include drowsiness and dizziness.",
}

side_effects_db: Dict[str, Dict[str, List[str]]] = {
    "Amoxicillin": {
        "initial": ["nausea", "diarrhea", "rash"],
        "long_term": ["yeast infections", "antibiotic resistance"],
    },
    "Lisinopril": {
        "initial": ["dizziness", "dry cough"],
        "long_term": ["kidney function changes", "high potassium levels"],
    },
    "Metformin": {
        "initial": ["stomach upset", "metallic taste"],
        "long_term": ["vitamin B12 deficiency", "lactic acidosis (rare)"],
    },
    "Atorvastatin": {
        "initial": ["headache", "abdominal pain"],
        "long_term": ["muscle pain", "liver enzyme elevations"],
    },
    "Omeprazole": {
        "initial": ["headache", "constipation"],
        "long_term": ["bone density loss", "risk of C. difficile infection"],
    },
    "Amlodipine": {
        "initial": ["swelling of ankles", "flushing"],
        "long_term": ["gum overgrowth", "low blood pressure"],
    },
    "Levothyroxine": {
        "initial": ["increased appetite", "sweating"],
        "long_term": ["bone loss (if over‑treated)", "arrhythmias"],
    },
    "Simvastatin": {
        "initial": ["constipation", "stomach cramps"],
        "long_term": ["muscle weakness", "rare rhabdomyolysis"],
    },
    "Losartan": {
        "initial": ["dizziness", "fatigue"],
        "long_term": ["elevated potassium", "kidney function decline"],
    },
    "Hydrochlorothiazide": {
        "initial": ["increased urination", "dizziness"],
        "long_term": ["low sodium/potassium", "gout flare‑ups"],
    },
    "Gabapentin": {
        "initial": ["drowsiness", "dizziness"],
        "long_term": ["weight gain", "peripheral edema"],
    },
    # add more as needed
}

interactions_db: Dict[frozenset, str] = {
    frozenset(["Amoxicillin", "Warfarin"]): "May increase risk of bleeding.",
    frozenset(
        ["Lisinopril", "Ibuprofen"]
    ): "Ibuprofen can reduce Lisinopril’s blood pressure effect.",
    frozenset(
        ["Metformin", "Cimetidine"]
    ): "Cimetidine can decrease Metformin clearance, increasing risk of side effects.",
    frozenset(
        ["Atorvastatin", "Erythromycin"]
    ): "Erythromycin may raise Atorvastatin levels and risk of muscle toxicity.",
    frozenset(
        ["Omeprazole", "Clopidogrel"]
    ): "Omeprazole can reduce Clopidogrel activation, lowering its efficacy.",
    frozenset(
        ["Amlodipine", "Simvastatin"]
    ): "Amlodipine may increase Simvastatin concentration slightly; monitor for muscle pain.",
    frozenset(
        ["Levothyroxine", "Calcium Supplements"]
    ): "Calcium can interfere with Levothyroxine absorption; separate dosing by 4 hours.",
    frozenset(
        ["Losartan", "Potassium Supplements"]
    ): "Risk of hyperkalemia when combined.",
    frozenset(
        ["Hydrochlorothiazide", "Lithium"]
    ): "HCTZ can increase Lithium levels, risk of toxicity.",
    frozenset(
        ["Gabapentin", "Opioids"]
    ): "Additive CNS depression; monitor for sedation.",
}


class AuthenticateArgs(TypedDict):
    first_name: str
    last_name: str
    phone_number: str


class ScheduleAppointmentArgs(TypedDict, total=False):
    patient_name: str
    dob: str  # ISO format: YYYY-MM-DD
    appointment_type: str
    preferred_date: str
    preferred_time: str


class RefillPrescriptionArgs(TypedDict, total=False):
    patient_name: str
    medication_name: str
    pharmacy: str


class LookupMedicationArgs(TypedDict):
    medication_name: str


class PAArgs(TypedDict):
    patient_info: Dict[str, Any]
    physician_info: Dict[str, Any]
    clinical_info: Dict[str, Any]
    treatment_plan: Dict[str, Any]
    policy_text: str


class EscalateEmergencyArgs(TypedDict):
    reason: str


class FillNewPrescriptionArgs(TypedDict):
    patient_name: str
    medication_name: str
    dosage: str
    pharmacy: str


class SideEffectsArgs(TypedDict):
    medication_name: str


class GetCurrentPrescriptionsArgs(TypedDict):
    patient_name: str


class CheckDrugInteractionsArgs(TypedDict):
    new_medication: str
    current_medications: List[str]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def _json(ok: bool, msg: str, **data):
    return json.dumps(
        {"ok": ok, "message": msg, "data": data or None}, ensure_ascii=False
    )


# ---------------------------------------------------------------------------
# Public tool functions
# ---------------------------------------------------------------------------
async def authenticate_user(args: AuthenticateArgs) -> Dict[str, Any]:
    first = args["first_name"].strip().title()
    last = args["last_name"].strip().title()
    phone = args["phone_number"].strip()
    full = f"{first} {last}"

    logger.info(f"🔎 Checking user: {full} with phone: {phone}")

    rec = patients_db.get(full)
    if not rec:
        logger.warning(f"❌ No record for name: {full}")
        return {
            "authenticated": False,
            "message": f"Name '{full}' not found.",
            "patient_id": None,
        }

    stored_phone = rec["phone"].replace("-", "").strip()
    phone = phone.replace("-", "").strip()

    logger.info(f"📞 Cleaned stored phone: {stored_phone}")
    logger.info(f"📞 Cleaned input phone:  {phone}")

    if stored_phone == phone:
        logger.info(f"✅ Authentication succeeded for {full}")
        return {
            "authenticated": True,
            "message": f"Authenticated {full}.",
            "patient_id": rec["patient_id"],
            "first_name": first,
            "last_name": last,
            "phone_number": phone,
        }
    else:
        logger.warning(
            f"❌ Phone mismatch for {full}: expected {stored_phone}, got {phone}"
        )
        return {
            "authenticated": False,
            "message": "Authentication failed – name or phone mismatch.",
            "patient_id": None,
        }


async def schedule_appointment(args: ScheduleAppointmentArgs) -> str:
    name = args.get("patient_name", "")
    dob = args.get("dob", "")
    if not name or not dob:
        return _json(False, "Missing patient name or date of birth.")
    rec = patients_db.get(name)
    if not rec or rec["dob"] != dob:
        return _json(False, "Patient not found or DOB mismatch.")

    appt = args.get("appointment_type") or ""
    if not appt:
        return _json(False, "Missing appointment type.")
    date_str = args.get("preferred_date") or str(_date.today() + _timedelta(days=3))
    time_str = args.get("preferred_time") or "14:00"

    return _json(
        True,
        f"Appointment booked for {name} on {date_str} at {time_str}.",
        date=date_str,
        time=time_str,
        appointment_type=appt,
    )


async def refill_prescription(args: RefillPrescriptionArgs) -> str:
    name = args.get("patient_name", "")
    med = args.get("medication_name", "")
    if not name or not med:
        return _json(False, "Missing patient name or medication name.")

    user_rx = prescriptions_db.get(name, {})
    if med not in user_rx:
        return _json(False, f"No active prescription for {med} under {name}.")

    pharm = args.get("pharmacy") or user_rx[med]["pharmacy"]
    return _json(
        True, f"Refill placed for {med} to {pharm}.", pharmacy=pharm, medication=med
    )


async def lookup_medication_info(args: LookupMedicationArgs) -> str:
    med = args["medication_name"].strip().title()
    if not med:
        return _json(False, "Medication name is required.")
    info = medications_info_db.get(med)
    if not info:
        return _json(False, f"No information found for {med}.")
    return _json(True, f"Information on {med}.", summary=info)


async def evaluate_prior_authorization(args: PAArgs) -> str:
    plan = args["treatment_plan"].get("requested_medication", "")
    if not plan:
        return _json(False, "Requested medication is missing in treatment plan.")
    return _json(True, f"Prior authorization for {plan} auto‑approved.")


async def escalate_emergency(args: EscalateEmergencyArgs) -> str:
    reason = args["reason"].strip()
    if not reason:
        return _json(False, "Reason for escalation is required.")
    return _json(True, "Emergency escalation triggered.", reason=reason)


async def fill_new_prescription(args: FillNewPrescriptionArgs) -> str:
    name = args.get("patient_name", "")
    med = args.get("medication_name", "").title()
    dose = args.get("dosage", "")
    pharm = args.get("pharmacy", "")
    if not (name and med and dose and pharm):
        return _json(
            False, "Missing patient_name, medication_name, dosage, or pharmacy."
        )

    prescriptions_db.setdefault(name, {})[med] = {
        "last_refill": str(_date.today()),
        "pharmacy": pharm,
        "dosage": dose,
    }
    return _json(
        True,
        f"New prescription for {med} ({dose}) added for {name} at {pharm}.",
        medication=med,
        dosage=dose,
        pharmacy=pharm,
    )


async def lookup_side_effects(args: SideEffectsArgs) -> str:
    med = args["medication_name"].strip().title()
    if not med:
        return _json(False, "Medication name is required.")
    info = side_effects_db.get(med)
    if not info:
        return _json(False, f"No side‑effect data found for {med}.")
    return _json(
        True,
        f"Side effects for {med}.",
        initial=info["initial"],
        long_term=info["long_term"],
    )


async def get_current_prescriptions(args: GetCurrentPrescriptionsArgs) -> str:
    name = args.get("patient_name", "")
    if not name:
        return _json(False, "Patient name is required.")
    rx = prescriptions_db.get(name, {})
    if not rx:
        return _json(False, f"No active prescriptions found for {name}.")
    meds = [{"medication": m, **details} for m, details in rx.items()]
    return _json(True, f"Active prescriptions for {name}.", prescriptions=meds)


async def check_drug_interactions(args: CheckDrugInteractionsArgs) -> str:
    new_med = args.get("new_medication", "").title()
    current = args.get("current_medications", [])
    if not new_med or not current:
        return _json(False, "Both new_medication and current_medications are required.")
    found: Dict[str, str] = {}
    for existing in current:
        pair = frozenset([new_med, existing])
        if pair in interactions_db:
            found[existing] = interactions_db[pair]
    if not found:
        return _json(
            True,
            f"No known interactions between {new_med} and your current medications.",
        )
    return _json(True, f"Potential interactions for {new_med}:", interactions=found)
