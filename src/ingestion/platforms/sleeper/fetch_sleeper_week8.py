
import csv
import json
import urllib.request
from pathlib import Path

LEAGUE_ID = "1312103420571705344"
WEEK = 8

BASE_URL = "https://api.sleeper.app/v1"

def fetch_json(url):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "DynastyDecisionEngine/1.0"}
    )
    with urllib.request.urlopen(request) as response:
        return json.loads(response.read().decode("utf-8"))

# Retrieve actual Sleeper matchup results
matchups = fetch_json(
    f"{BASE_URL}/league/{LEAGUE_ID}/matchups/{WEEK}"
)

# Retrieve league rosters and user information
rosters = fetch_json(
    f"{BASE_URL}/league/{LEAGUE_ID}/rosters"
)

users = fetch_json(
    f"{BASE_URL}/league/{LEAGUE_ID}/users"
)

# Map roster IDs to team owners
roster_owners = {
    roster["roster_id"]: roster.get("owner_id")
    for roster in rosters
}

user_names = {
    user["user_id"]: (
        user.get("metadata", {}).get("team_name")
        or user.get("display_name")
        or user["user_id"]
    )
    for user in users
}

# Save the original API response for auditing
output_dir = Path("sleeper_validation")
output_dir.mkdir(exist_ok=True)

with open(
    output_dir / f"week_{WEEK}_matchups_raw.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(matchups, file, indent=2)

# Flatten rostered player scores
rows = []

for matchup in matchups:
    roster_id = matchup["roster_id"]
    owner_id = roster_owners.get(roster_id)

    player_points = matchup.get("players_points") or {}
    starters = set(matchup.get("starters") or [])

    for sleeper_player_id, points in player_points.items():
        rows.append({
            "season": 2026,
            "week": WEEK,
            "roster_id": roster_id,
            "team_name": user_names.get(owner_id, "Unknown"),
            "matchup_id": matchup.get("matchup_id"),
            "sleeper_player_id": sleeper_player_id,
            "is_starter": sleeper_player_id in starters,
            "sleeper_points": points
        })

csv_path = output_dir / f"week_{WEEK}_player_scores.csv"

with open(csv_path, "w", newline="", encoding="utf-8") as file:
    if rows:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

print(f"Matchup records: {len(matchups)}")
print(f"Player score records: {len(rows)}")
print(f"Unique Sleeper players: {len(set(r['sleeper_player_id'] for r in rows))}")
print(f"Starters: {sum(r['is_starter'] for r in rows)}")
print(f"Saved: {csv_path}")
print(f"Raw response: {output_dir / f'week_{WEEK}_matchups_raw.json'}")
