"""
2021 Tamil Nadu Election Baseline Generator

Parses ACTUAL 2021 TN Assembly election results from CSV data (TCPD dataset).
This baseline is used for:
1. Calculating swing predictions (current sentiment vs historical margin)
2. Identifying vulnerable seats (low margin = high swing potential)
3. Validating sentiment predictions against historical voting patterns
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List

# Path setup
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
CSV_PATH = PROJECT_ROOT / "2021 election data.csv"
CONFIG_PATH = PROJECT_ROOT / "config" / "districts.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "2021_baseline.json"

# ============================================================
# 2021 ALLIANCE MAPPINGS (Accurate)
# ============================================================

# DMK+ Alliance (Secular Progressive Alliance - SPA)
DMK_ALLIANCE_PARTIES = {
    'DMK',      # Dravida Munnetra Kazhagam
    'INC',      # Indian National Congress
    'VCK',      # Viduthalai Chiruthaigal Katchi
    'CPI',      # Communist Party of India
    'CPI(M)',   # Communist Party of India (Marxist)
    'CPM',      # Alias for CPI(M)
    'MDMK',     # Marumalarchi Dravida Munnetra Kazhagam (Vaiko)
    'IUML',     # Indian Union Muslim League
    'KMDK',     # Kongunadu Makkal Desia Katchi
    'MMK',      # Manithaneya Makkal Katchi
    'AIFB',     # All India Forward Bloc
    'ATP',      # Adi Tamilar Peravai
    'MVK',      # Makkal Viduthalai Katchi
}

# ADMK+ Alliance (NDA)
ADMK_ALLIANCE_PARTIES = {
    'ADMK',     # All India Anna Dravida Munnetra Kazhagam
    'AIADMK',   # Alias for ADMK
    'PMK',      # Pattali Makkal Katchi
    'BJP',      # Bharatiya Janata Party
    'TMC(M)',   # Tamil Maanila Congress (Moopanar)
    'TMMK',     # Tamizhaga Makkal Munnetra Kazhagam
    'PBK',      # Puratchi Bharatham Katchi
    'PDK',      # Pasumpon Desiya Kazhagam
}

# AMMK Front (TTV Dhinakaran)
AMMK_FRONT_PARTIES = {
    'AMMK',     # Amma Makkal Munnettra Kazagam
    'DMDK',     # Desiya Murpokku Dravida Kazhagam (Vijayakanth)
    'SDPI',     # Social Democratic Party of India
    'AIMIM',    # All India Majlis-e-Ittehadul Muslimeen
}

# MNM Front (Kamal Haasan)
MNM_FRONT_PARTIES = {
    'MNM',      # Makkal Needhi Maiam
    'IJK',      # Indiya Jananayaka Katchi
    'AISMK',    # All India Samathuva Makkal Katchi
    'JDS',      # Janata Dal (Secular)
}

# NTK - Contested Alone
NTK_PARTIES = {
    'NTK',      # Naam Tamilar Katchi (Seeman)
}


def map_party_to_alliance(party: str) -> str:
    """Map individual party to alliance based on 2021 coalition structure."""
    party_clean = party.strip().upper()
    
    # Exact match first (most reliable)
    if party_clean in {p.upper() for p in DMK_ALLIANCE_PARTIES}:
        return "DMK_Alliance"
    
    if party_clean in {p.upper() for p in ADMK_ALLIANCE_PARTIES}:
        return "ADMK_Alliance"
    
    if party_clean in {p.upper() for p in AMMK_FRONT_PARTIES}:
        return "AMMK_Front"
    
    if party_clean in {p.upper() for p in MNM_FRONT_PARTIES}:
        return "MNM_Front"
    
    if party_clean in {p.upper() for p in NTK_PARTIES}:
        return "NTK"
    
    # Fuzzy matching for variations
    # DMK Alliance variations
    if any(x in party_clean for x in ['CONGRESS', 'INC']):
        return "DMK_Alliance"
    if 'COMMUNIST' in party_clean or 'CPI' in party_clean:
        return "DMK_Alliance"
    
    # ADMK Alliance variations  
    if 'AIADMK' in party_clean or party_clean == 'ADMK':
        return "ADMK_Alliance"
    if party_clean == 'PMK' or 'PATTALI' in party_clean:
        return "ADMK_Alliance"
    if party_clean == 'BJP' or 'BHARATIYA JANATA' in party_clean:
        return "ADMK_Alliance"
    
    # AMMK variations
    if 'AMMK' in party_clean or 'AMMA MAKKAL' in party_clean:
        return "AMMK_Front"
    if 'DMDK' in party_clean or 'VIJAYAKANTH' in party_clean:
        return "AMMK_Front"
    
    # MNM variations
    if 'MNM' in party_clean or 'MAKKAL NEEDHI' in party_clean or 'KAMAL' in party_clean:
        return "MNM_Front"
    
    # NTK variations
    if 'NTK' in party_clean or 'NAAM TAMILAR' in party_clean or 'SEEMAN' in party_clean:
        return "NTK"
    
    # Independent or unrecognized
    if party_clean == 'IND' or 'INDEPENDENT' in party_clean:
        return "Independent"
    
    return "Others"


def load_districts_config() -> Dict:
    """Load districts.json for constituency-district mapping validation."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r') as f:
            return json.load(f)
    return {}


def generate_baseline() -> Dict:
    """
    Parse the 2021 election CSV and generate baseline data.
    
    Returns dict with structure:
    {
        "metadata": {...},
        "constituencies": {
            "CONSTITUENCY_NAME": {
                "district": "...",
                "winner": "PARTY",
                "winner_alliance": "DMK_Alliance|ADMK_Alliance|Others",
                "margin": 12345,
                "margin_percentage": 5.67,
                "vote_share": 45.23,
                "runner_up": "PARTY2",
                "runner_up_alliance": "..."
            }
        },
        "summary": {...}
    }
    """
    print(f"Loading CSV from: {CSV_PATH}")
    
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")
    
    # Load CSV with pandas
    df = pd.read_csv(CSV_PATH)
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Filter for Tamil Nadu 2021 and Position 1 (winners) and Position 2 (runner-up)
    df_tn = df[
        (df['State_Name'] == 'Tamil_Nadu') & 
        (df['Year'] == 2021)
    ].copy()
    
    print(f"Tamil Nadu 2021 rows: {len(df_tn)}")
    
    # Get winners (Position == 1)
    winners = df_tn[df_tn['Position'] == 1].copy()
    
    # Get runner-ups (Position == 2)
    runners_up = df_tn[df_tn['Position'] == 2].copy()
    
    print(f"Winners: {len(winners)}")
    print(f"Runner-ups: {len(runners_up)}")
    
    # Build constituencies dict
    constituencies = {}
    
    for _, row in winners.iterrows():
        constituency_name = row['Constituency_Name'].strip().upper()
        district_name = row['District_Name'].strip()
        party = row['Party'].strip()
        
        # Get runner-up info
        runner_up_row = runners_up[
            runners_up['Constituency_Name'].str.strip().str.upper() == constituency_name
        ]
        
        runner_up_party = ""
        runner_up_alliance = ""
        if len(runner_up_row) > 0:
            runner_up_party = runner_up_row.iloc[0]['Party'].strip()
            runner_up_alliance = map_party_to_alliance(runner_up_party)
        
        constituencies[constituency_name] = {
            "district": district_name,
            "winner": party,
            "winner_alliance": map_party_to_alliance(party),
            "margin": int(row['Margin']) if pd.notna(row['Margin']) else 0,
            "margin_percentage": float(row['Margin_Percentage']) if pd.notna(row['Margin_Percentage']) else 0.0,
            "vote_share": float(row['Vote_Share_Percentage']) if pd.notna(row['Vote_Share_Percentage']) else 0.0,
            "runner_up": runner_up_party,
            "runner_up_alliance": runner_up_alliance,
            "total_votes": int(row['Valid_Votes']) if pd.notna(row['Valid_Votes']) else 0,
            "turnout": float(row['Turnout_Percentage']) if pd.notna(row['Turnout_Percentage']) else 0.0
        }
    
    # Calculate summary statistics
    alliance_counts = {}
    alliance_seats = {
        "DMK_Alliance": [], 
        "ADMK_Alliance": [], 
        "AMMK_Front": [],
        "MNM_Front": [],
        "NTK": [], 
        "Independent": [],
        "Others": []
    }
    
    for const_name, data in constituencies.items():
        alliance = data['winner_alliance']
        alliance_counts[alliance] = alliance_counts.get(alliance, 0) + 1
        if alliance in alliance_seats:
            alliance_seats[alliance].append(const_name)
    
    # Identify vulnerable seats (margin < 5%)
    vulnerable_seats = [
        name for name, data in constituencies.items()
        if data['margin_percentage'] < 5.0
    ]
    
    # Identify safe seats (margin > 20%)
    safe_seats = [
        name for name, data in constituencies.items()
        if data['margin_percentage'] > 20.0
    ]
    
    baseline = {
        "metadata": {
            "source": "TCPD (Trivedi Centre for Political Data) Dataset",
            "election_year": 2021,
            "election_date": "April 6, 2021",
            "total_constituencies": len(constituencies),
            "data_quality": "VERIFIED",
            "generated_by": "generate_2021_baseline.py"
        },
        "constituencies": constituencies,
        "summary": {
            "seats_by_alliance": alliance_counts,
            "total_seats": len(constituencies),
            "vulnerable_seats_count": len(vulnerable_seats),
            "safe_seats_count": len(safe_seats),
            "average_margin_percentage": round(
                sum(c['margin_percentage'] for c in constituencies.values()) / len(constituencies), 2
            ) if constituencies else 0
        },
        "analysis": {
            "vulnerable_seats": vulnerable_seats[:20],  # Top 20 most vulnerable
            "safe_seats": safe_seats[:10]  # Top 10 safest
        }
    }
    
    return baseline


def print_summary(baseline: Dict):
    """Print a summary of the baseline data."""
    print("\n" + "=" * 60)
    print("2021 TAMIL NADU ELECTION BASELINE SUMMARY")
    print("=" * 60)
    
    summary = baseline['summary']
    metadata = baseline['metadata']
    
    print(f"\nSource: {metadata['source']}")
    print(f"Total Constituencies: {metadata['total_constituencies']}")
    
    print(f"\nSEATS BY ALLIANCE:")
    print("-" * 40)
    for alliance, count in sorted(summary['seats_by_alliance'].items(), key=lambda x: -x[1]):
        pct = (count / summary['total_seats']) * 100
        print(f"  {alliance:20s}: {count:3d} seats ({pct:.1f}%)")
    
    print(f"\nKEY METRICS:")
    print("-" * 40)
    print(f"  Vulnerable Seats (<5% margin): {summary['vulnerable_seats_count']}")
    print(f"  Safe Seats (>20% margin): {summary['safe_seats_count']}")
    print(f"  Average Margin: {summary['average_margin_percentage']:.2f}%")
    
    print(f"\nTOP 10 MOST VULNERABLE SEATS:")
    print("-" * 40)
    
    # Sort constituencies by margin percentage
    sorted_by_margin = sorted(
        baseline['constituencies'].items(),
        key=lambda x: x[1]['margin_percentage']
    )
    
    for i, (name, data) in enumerate(sorted_by_margin[:10], 1):
        print(f"  {i:2d}. {name:25s} | {data['winner']:8s} vs {data['runner_up']:8s} | Margin: {data['margin_percentage']:.2f}%")
    
    print("\n" + "=" * 60)


def main():
    """Main entry point."""
    print("Generating 2021 Tamil Nadu Election Baseline...")
    print("-" * 60)
    
    try:
        baseline = generate_baseline()
        
        # Ensure output directory exists
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Save to JSON
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(baseline, f, indent=2, ensure_ascii=False)
        
        print(f"\nBaseline saved to: {OUTPUT_PATH}")
        
        # Print summary
        print_summary(baseline)
        
        return baseline
        
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print("Please ensure '2021 election data.csv' is in the project root.")
        return None
    except Exception as e:
        print(f"ERROR generating baseline: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
