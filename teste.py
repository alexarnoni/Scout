import requests, json

BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/bra.1"
resp = requests.get(f"{BASE}/scoreboard", params={"dates": "20260312"}, timeout=10)

for event in resp.json().get("events", []):
    for c in event["competitions"][0]["competitors"]:
        team = c["team"]
        print(f"{team['displayName']}: {team.get('logo', 'SEM LOGO')}")
    break