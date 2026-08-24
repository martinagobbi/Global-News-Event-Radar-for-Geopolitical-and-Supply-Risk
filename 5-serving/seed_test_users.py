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
        "display_name": "Electronics & Semiconductors (test)",
        "territories": [
            "Taiwan", "South Korea", "Japan", "China", "Malaysia", "Singapore",
            "Philippines", "Vietnam", "Thailand", "Indonesia", "India",
            "Hong Kong", "Macau", "Cambodia", "Laos", "Myanmar", "Brunei",
            "Mongolia", "Bangladesh", "Sri Lanka",
        ],
        "keywords": {
            "sourcing": [
                "silicon wafers", "gallium arsenide", "neodymium magnets",
                "tantalum capacitors", "lithium hexafluorophosphate", "photoresist",
                "sputtering targets", "copper clad laminate", "indium tin oxide",
                "rare earth oxides", "germanium ingots", "palladium paste",
                "epoxy molding compound", "bonding wire", "ceramic substrates",
                "quartz crystals", "cobalt cathode", "graphite anode",
                "polyimide film", "high purity argon",
            ],
            "manufacturing": [
                "photolithography steppers", "wafer dicing saws", "die bonders",
                "reflow ovens", "pick and place machines", "plasma etchers",
                "chemical vapour deposition", "wire bonders", "surface mount assembly",
                "burn-in boards", "test handlers", "solder paste printers",
                "cleanroom filters", "ion implanters", "chemical mechanical polishing",
                "mask aligners", "thermal shock chambers", "ultrasonic cleaners",
                "laser trimmers", "conformal coating",
            ],
            "storage": [
                "antistatic trays", "dry cabinets", "moisture barrier bags",
                "desiccant packs", "humidity indicator cards", "JEDEC matrix trays",
                "reel packaging", "ESD shelving", "nitrogen cabinets",
                "vacuum sealers", "wafer cassettes", "FOUP carriers",
                "tape and reel", "bonded warehouse Shenzhen", "chip magazines",
                "foam inserts", "static shielding film", "climate controlled vault",
                "component carousels", "kitting bins",
            ],
            "delivery": [
                "air freight Taipei", "bonded trucking Shenzhen",
                "express courier Hsinchu", "sea freight Kaohsiung",
                "consolidated LCL Penang", "temperature logged parcels",
                "customs brokerage Singapore", "cross dock Hong Kong",
                "milk run Suzhou", "chartered cargo flight", "last mile Seoul",
                "freight forwarding Osaka", "transhipment Port Klang",
                "rail freight Chongqing", "drayage Yokohama", "courier Manila",
                "air charter Incheon", "bonded rail Xian", "groupage Bangkok",
                "expedited Shanghai",
            ],
            "companies": [
                "TSMC", "ASML", "Tokyo Electron", "SK Hynix", "MediaTek",
                "Foxconn", "Renesas", "Infineon", "Murata", "Nidec", "AUO",
                "Advantest", "Amkor", "ASE Technology", "Kioxia", "Rohm",
                "Yageo", "Delta Electronics", "Wistron", "Pegatron",
            ],
        },
    },
    "radar_pharma": {
        "display_name": "Pharmaceuticals & Biologics (test)",
        "territories": [
            "Germany", "France", "Italy", "Spain", "Switzerland", "Belgium",
            "Netherlands", "Ireland", "Denmark", "Sweden", "Norway", "Finland",
            "Austria", "Poland", "Czech Republic", "Hungary", "Portugal",
            "Greece", "Slovenia", "Slovakia",
        ],
        "keywords": {
            "sourcing": [
                "paracetamol API", "ibuprofen API", "amoxicillin trihydrate",
                "lactose monohydrate", "microcrystalline cellulose",
                "magnesium stearate", "povidone K30", "titanium dioxide USP",
                "hypromellose", "sodium starch glycolate", "gelatin capsules",
                "water for injection", "crude heparin", "insulin crystals",
                "plasmid DNA", "cell culture media", "single use bioreactor bags",
                "chromatography resin", "type I glass vials", "bromobutyl stoppers",
            ],
            "manufacturing": [
                "tablet presses", "fluid bed dryers", "blister packaging lines",
                "lyophilizers", "autoclaves", "isolator cabinets",
                "high shear granulators", "coating pans", "capsule fillers",
                "vial washers", "depyrogenation tunnels", "aseptic filling lines",
                "tangential flow filtration", "cleanroom garments",
                "HEPA filter units", "WFI stills", "sterility test kits",
                "endotoxin assays", "HPLC columns", "stability chambers",
            ],
            "storage": [
                "cold chain warehouse Basel", "2-8C refrigerated units",
                "cryogenic dewars", "controlled substance vaults", "quarantine bays",
                "validated freezers", "dry ice pellets", "pharma thermal blankets",
                "GDP compliant depot", "temperature mapping loggers",
                "ultra low freezers", "humidity controlled rooms",
                "serialisation aggregation", "narcotics safe", "retained sample store",
                "pallet shippers", "phase change materials", "insulated pharma boxes",
                "batch quarantine racks", "recall hold area",
            ],
            "delivery": [
                "GDP validated trucking", "cold chain courier Frankfurt",
                "pharma air freight Brussels", "temperature controlled van",
                "dedicated ambient trailer", "direct to pharmacy",
                "hospital consignment", "clinical trial kits",
                "cross border EU pharma", "night distribution",
                "wholesaler replenishment", "express medical courier",
                "dry ice replenishment", "active container rental",
                "secure narcotics transport", "patient direct shipment",
                "depot to depot transfer", "airport pharma handling",
                "last mile clinic", "emergency stock release",
            ],
            "companies": [
                "Novartis", "Roche", "Sanofi", "Bayer", "Lonza", "Novo Nordisk",
                "GSK", "AstraZeneca", "Boehringer Ingelheim", "Merck KGaA",
                "Teva", "Recordati", "Chiesi", "Menarini", "Almirall",
                "Servier", "Ipsen", "UCB", "Orion Pharma", "Grifols",
            ],
        },
    },
    "radar_agrifood": {
        "display_name": "Agri-food Commodities (test)",
        "territories": [
            "Brazil", "Argentina", "Chile", "Peru", "Colombia", "Ecuador",
            "Mexico", "United States", "Canada", "Uruguay", "Paraguay",
            "Bolivia", "Ivory Coast", "Ghana", "Nigeria", "Kenya", "Ethiopia",
            "Tanzania", "South Africa", "Morocco",
        ],
        "keywords": {
            "sourcing": [
                "arabica green coffee", "robusta cherries", "cocoa beans",
                "raw cane sugar", "soybean meal", "yellow maize", "durum wheat",
                "palm kernel oil", "cashew kernels", "black pepper", "vanilla pods",
                "shea butter", "tilapia fillets", "beef carcasses", "milk powder",
                "orange concentrate", "banana cartons", "hass avocado",
                "table grapes", "quinoa grain",
            ],
            "manufacturing": [
                "roasting drums", "cocoa grinders", "sugar centrifuges",
                "oil expellers", "flour mills", "pasteurisers", "retort sterilisers",
                "canning lines", "food freeze dryers", "extrusion cookers",
                "blanchers", "optical sorting graders", "dehullers",
                "tempering machines", "filling capping lines", "spray dryers",
                "homogenisers", "smokehouses", "brine injectors", "vacuum tumblers",
            ],
            "storage": [
                "grain silos Santos", "controlled atmosphere rooms",
                "banana ripening chambers", "cocoa warehouses Abidjan",
                "coffee bonded stores", "blast freezers", "chilled meat lockers",
                "bulk liquid tanks", "fumigation chambers", "hermetic grain bags",
                "jute sack stacks", "reefer plug points", "dry bulk sheds",
                "ambient FMCG depot", "phytosanitary quarantine store",
                "silo aeration systems", "molasses tank farm",
                "cold store Valparaiso", "palletised dry goods",
                "grain humidity monitors",
            ],
            "delivery": [
                "reefer container shipping", "bulk carrier charter",
                "breakbulk sacks", "coastal grain barge",
                "refrigerated trucking Brazil", "phytosanitary clearance",
                "port silo loading", "containerised coffee",
                "air freight perishables Nairobi", "Chile fruit cold chain",
                "inland waterway soy", "rail grain corridor",
                "transloading Rosario", "West Africa feeder vessel",
                "drayage Santos", "groupage cocoa", "chartered reefer vessel",
                "bulk vegetable oil tanker", "cross border trucking Mercosur",
                "export terminal loading",
            ],
            "companies": [
                "Cargill", "Bunge", "Louis Dreyfus", "ADM", "Olam",
                "Barry Callebaut", "JBS", "Marfrig", "Fresh Del Monte", "Dole",
                "Chiquita", "Sucden", "ECOM Agroindustrial", "Golden Agri",
                "Wilmar", "Tereos", "Copersucar", "Amaggi", "SLC Agricola",
                "Nutrien",
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
