#!/usr/bin/env python
"""
Seed the three fixed test accounts into MongoDB through the serving backend.

Run once, after the stores and the backend are up:

    python 5-serving/seed_test_users.py                       # localhost:8000
    BACKEND_URL=http://operator-host:8000 python 5-serving/seed_test_users.py

Idempotent: PUT /users/{id}/profile replaces the profile, so re-running resets
the three accounts to this known state. Nothing else in Mongo is touched.

Accounts are mutually exclusive. Each profile models a
different supply chain in a different part of the world:

    radar_electronics — semiconductors / electronics, Asia-Pacific
    radar_pharma      — pharmaceuticals / biologics, Europe
    radar_agrifood    — agri-food commodities, Americas + Africa
"""
from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import combinations

import requests

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000").rstrip("/")
MIN_ANSWERS = 20
# How long to wait for the backend before giving up, and how often to re-check.
BACKEND_WAIT_SECONDS = int(os.getenv("BACKEND_WAIT_SECONDS", "120"))
BACKEND_RETRY_SECONDS = int(os.getenv("BACKEND_RETRY_SECONDS", "5"))

# ── Profiles ────────────────────────────────────────────────────────────────

PROFILES: dict[str, dict] = {
    "radar_electronics": {
        "display_name": "Electronics & Manufacturing (test)",
        "territories": [
            "China", "Taiwan", "Japan", "South Korea", "Vietnam", "Malaysia", "Thailand",
            "Singapore", "Philippines", "Indonesia", "Hong Kong", "Australia", "Cambodia",
            "Myanmar", "Laos", "Mongolia", "New Zealand", "Brunei", "Timor-Leste",
            "Papua New Guinea"
        ],
        "keywords": {
            "sourcing": [
                "semiconductor", "chip", "wafer", "lithium", "cobalt", "nickel", "copper",
                "aluminium", "rare earth", "polysilicon", "graphite", "magnet",
                "battery cell", "circuit board", "display panel", "sensor", "capacitor",
                "resistor", "alloy", "titanium"
            ],
            "manufacturing": [
                "factory", "manufacturing", "production", "assembly", "foundry",
                "fabrication", "machinery", "robotics", "tooling", "moulding",
                "packaging line", "automation", "precision", "components", "subassembly",
                "calibration", "clean room", "chip plant", "production line",
                "quality control"
            ],
            "storage": [
                "bonded warehouse", "distribution centre", "stockroom", "component store",
                "buffer stock", "safety stock", "consignment stock", "kitting", "racking",
                "pallet store", "spare parts depot", "dry store", "antistatic store",
                "staging area", "quarantine store", "finished goods", "raw material store",
                "bin location", "cycle count", "inbound dock"
            ],
            "delivery": [
                "tariff", "sanction", "export", "import", "embargo", "customs", "quota",
                "levy", "clearance", "air freight", "courier", "consolidation",
                "transhipment", "drayage", "cross-dock", "forwarder", "bill of lading",
                "incoterms", "lead time", "export licence"
            ],
            "companies": [
                "TSMC", "Samsung", "Foxconn", "SK Hynix", "MediaTek", "Sony", "Panasonic",
                "Toyota", "Hyundai", "Xiaomi", "Huawei", "Lenovo", "BYD", "Infineon",
                "Renesas", "Murata", "ASE Technology", "Wistron", "Pegatron", "Nidec"
            ],
        },
    },
    "radar_pharma": {
        "display_name": "Pharmaceuticals & Healthcare (test)",
        "territories": [
            "India", "Ireland", "Switzerland", "Germany", "France", "Belgium", "Netherlands",
            "Denmark", "Sweden", "Austria", "Hungary", "Slovenia", "Czech Republic",
            "Poland", "Israel", "Pakistan", "Bangladesh", "Sri Lanka", "Jordan", "Egypt"
        ],
        "keywords": {
            "sourcing": [
                "paracetamol", "ibuprofen", "antibiotic", "vaccine", "insulin", "heparin",
                "excipient", "lactose", "cellulose", "gelatin capsule", "sterile water",
                "plasma", "reagent", "enzyme", "peptide", "antigen", "diluent", "solvent",
                "precursor", "active ingredient"
            ],
            "manufacturing": [
                "tablet press", "blister line", "lyophiliser", "autoclave", "isolator",
                "granulator", "coating pan", "capsule filler", "vial washer",
                "aseptic filling", "bioreactor", "chromatography", "filtration",
                "sterilisation", "clinical batch", "validation run", "cleanroom garment",
                "batch record", "potency test", "assay"
            ],
            "storage": [
                "cold chain warehouse", "refrigerated unit", "cryogenic dewar",
                "controlled substance vault", "quarantine bay", "validated freezer",
                "dry ice", "thermal blanket", "gdp depot", "temperature logger",
                "ultra low freezer", "humidity room", "serialisation", "narcotics safe",
                "retained sample", "pallet shipper", "phase change material",
                "insulated box", "batch quarantine", "recall hold"
            ],
            "delivery": [
                "hospital", "health", "medical", "drug", "doctor", "treatment", "healthcare",
                "patient", "medicine", "prescription", "pharmacy", "pharmaceutical",
                "clinical trial", "regulatory approval", "generic drug", "medical device",
                "wholesaler", "import permit", "batch release", "drug shortage"
            ],
            "companies": [
                "Novartis", "Roche", "Sanofi", "Bayer", "Lonza", "Novo Nordisk", "GSK",
                "AstraZeneca", "Boehringer", "Merck", "Teva", "Recordati", "Chiesi",
                "Menarini", "Almirall", "Servier", "Ipsen", "UCB", "Orion Pharma", "Grifols"
            ],
        },
    },
    "radar_agrifood": {
        "display_name": "Agri-food & Retail (test)",
        "territories": [
            "United States", "Canada", "Mexico", "Brazil", "Argentina", "Chile", "Colombia",
            "Peru", "Ecuador", "Uruguay", "United Kingdom", "Spain", "Italy", "Portugal",
            "Kenya", "Ghana", "South Africa", "Nigeria", "Ivory Coast", "Ethiopia"
        ],
        "keywords": {
            "sourcing": [
                "wheat", "grain", "coffee", "cocoa", "sugar", "soybean", "maize", "barley",
                "rice", "cotton", "palm oil", "dairy", "poultry", "beef", "seafood",
                "citrus", "cashew", "farmer", "farm", "drought"
            ],
            "manufacturing": [
                "mill", "bakery", "brewery", "dairy plant", "abattoir", "cannery",
                "roastery", "processing line", "pasteurisation", "fermentation", "milling",
                "bottling", "packing house", "supermarket", "curing", "drying", "grading",
                "sorting", "blending plant", "food processing"
            ],
            "storage": [
                "cold store", "grain silo", "chilled warehouse", "freezer", "ripening room",
                "fumigation", "granary", "bonded store", "ambient store", "humidity control",
                "pallet rack", "food security", "flood", "hopper", "controlled atmosphere",
                "warehouse space", "stock rotation", "shelf life", "retailer", "cool chain"
            ],
            "delivery": [
                "workforce", "reefer", "haulier", "distribution", "retail delivery",
                "last mile", "phytosanitary", "perishable", "cold chain", "groupage",
                "temperature control", "dispatch", "route planning", "food supply",
                "farm gate", "wholesale market", "export permit", "union", "crop yield",
                "food price"
            ],
            "companies": [
                "Nestle", "Unilever", "Danone", "Cargill", "Bunge", "Olam",
                "Barry Callebaut", "JBS", "Tesco", "Carrefour", "Sainsbury", "Aldi", "Lidl",
                "Mondelez", "Kraft Heinz", "Ferrero", "Lactalis", "Arla", "Tereos",
                "Nutrien"
            ],
        },
    },
}

COMMON = {
    "briefing_days": 30, 
    "older_news_days": 90, 
    "status": "registered",
    "timezone": "Europe/Rome",
}


# ── Self-checks ─────────────────────────────────────────────────────────────

def _all_terms(profile: dict) -> set[str]:
    """Every territory and keyword in a profile, lowercased for comparison."""
    terms = {t.lower() for t in profile["territories"]}
    for answers in profile["keywords"].values():
        terms |= {a.lower() for a in answers}
    return terms


def _check_mutually_exclusive() -> None:
    """Fail loudly if any two profiles share a territory or a keyword."""
    for a, b in combinations(PROFILES, 2):
        overlap = _all_terms(PROFILES[a]) & _all_terms(PROFILES[b])
        if overlap:
            sys.exit(f"ERROR: '{a}' and '{b}' share terms: {sorted(overlap)}")


def _check_minimum_answers() -> None:
    """Fail loudly if any question, or any territory list, is under MIN_ANSWERS."""
    for uid, profile in PROFILES.items():
        if len(profile["territories"]) < MIN_ANSWERS:
            sys.exit(f"ERROR: {uid} has {len(profile['territories'])} territories "
                     f"(minimum {MIN_ANSWERS})")
        for question, answers in profile["keywords"].items():
            if len(answers) < MIN_ANSWERS:
                sys.exit(f"ERROR: {uid}/{question} has {len(answers)} answers "
                         f"(minimum {MIN_ANSWERS})")
        for question, answers in profile["keywords"].items():
            if len(set(a.lower() for a in answers)) != len(answers):
                sys.exit(f"ERROR: {uid}/{question} contains duplicates")


def _check_territories_known() -> None:
    """Warn if a territory isn't in the backend's picker list (name mismatch)."""
    try:
        options = set(requests.get(f"{BACKEND_URL}/territories", timeout=20)
                      .json().get("territories", []))
    except requests.RequestException as exc:
        print(f"! Could not fetch /territories ({exc}); skipping name check.")
        return
    if not options:
        print("! Backend returned no territories; skipping name check.")
        return
    for uid, profile in PROFILES.items():
        unknown = [t for t in profile["territories"] if t not in options]
        if unknown:
            print(f"! {uid}: territories not in the picker list, so the "
                  f"processing layer will ignore them: {unknown}")


# ── Seeding ─────────────────────────────────────────────────────────────────

def _wait_for_backend() -> bool:
    """
    Block until the backend answers, or BACKEND_WAIT_SECONDS elapses.

    This is the only step of the setup that runs outside Docker, so it is the one
    most likely to be started a moment too early...
    """
    deadline = time.monotonic() + BACKEND_WAIT_SECONDS
    announced = False
    while True:
        try:
            requests.get(f"{BACKEND_URL}/health", timeout=5).raise_for_status()
            if announced:
                print(" ready.")
            return True
        except requests.RequestException:
            if time.monotonic() >= deadline:
                print(f"\nThe backend at {BACKEND_URL} did not respond within "
                      f"{BACKEND_WAIT_SECONDS}s. Is it running?")
                print("  docker compose --env-file .env.testing up -d --build")
                return False
            if not announced:
                print(f"waiting for the backend at {BACKEND_URL} ", end="", flush=True)
                announced = True
            print(".", end="", flush=True)
            time.sleep(BACKEND_RETRY_SECONDS)


def _put_profile(user_id: str, profile: dict) -> tuple[str, Exception | None]:
    payload = {"user_id": user_id, **COMMON, **profile}
    try:
        response = requests.put(
            f"{BACKEND_URL}/users/{user_id}/profile", json=payload, timeout=30
        )
        response.raise_for_status()
        return user_id, None
    except requests.RequestException as exc:
        return user_id, exc


def seed() -> int:
    if not _wait_for_backend():
        return 1
    _check_mutually_exclusive()
    _check_minimum_answers()
    _check_territories_known()

    # Fired CONCURRENTLY, not one PUT at a time. This is not about the network
    # round trips — those were never the cost. Each profile write makes the
    # processing layer's Mongo change-stream trigger recompute that user, and
    # sequential writes used to mean N separate recomputes, each rebuilding the
    # whole candidate catalogue from scratch (~90s of catalogue-build cost for
    # 3 users, measured, dominating the total). The trigger now DEBOUNCES
    # (see 4-processing/triggers.py, CHANGE_STREAM_DEBOUNCE_SECONDS): several
    # profile writes landing close together collapse into a single
    # recompute_all(), which is one catalogue build for all of them. Firing the
    # requests concurrently is what makes "close together" reliable — a
    # sequential loop only achieves it by accident, if every request happens to
    # complete faster than the debounce window.
    #
    # This is why parallelising the writes only became worth doing once the
    # trigger side could actually collapse them; sending 3 PUTs at once against
    # the OLD trigger would still have cost 3 independent recomputes, serialised
    # behind processing's advisory lock, in whatever order the change stream
    # happened to deliver them.
    failures = 0
    with ThreadPoolExecutor(max_workers=len(PROFILES)) as pool:
        futures = {
            pool.submit(_put_profile, uid, profile): (uid, profile)
            for uid, profile in PROFILES.items()
        }
        for future in as_completed(futures):
            user_id, profile = futures[future]
            _, exc = future.result()
            if exc is not None:
                print(f"FAILED {user_id}: {exc}")
                failures += 1
                continue
            n_kw = sum(len(v) for v in profile["keywords"].values())
            print(f"seeded {user_id}: {len(profile['territories'])} territories, "
                  f"{n_kw} keywords")
    return failures


if __name__ == "__main__":
    sys.exit(1 if seed() else 0)
