#-----------------------------------------------------------------------------------------------
#Insert into Player stats

import requests
import pandas as pd
from sqlalchemy import create_engine

# --- MySQL Connection ---
engine = create_engine("mysql+pymysql://root:Akshiya13@localhost:3306/cricbuzz")

# --- API Headers ---
HEADERS = {
    "x-rapidapi-key": "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}


# --- Helper: Normalize player entry ---
def extract_player(entry, country="Unknown"):
    if not entry or not isinstance(entry, dict) or not entry.get("id"):
        return None
    return {
        "player_id": int(entry["id"]),
        "full_name": entry.get("name", ""),
        "country": country,
        "playing_role": entry.get("role", ""),
        "batting_style": entry.get("battingStyle", ""),
        "bowling_style": entry.get("bowlingStyle", "")
    }


# --- Fetch and parse from multiple endpoints ---
def fetch_players_from_endpoints():
    endpoints = [
        "/stats/v1/player/trending",
        "/teams/v1/2/players",
        "/series/v1/3718/squads/15826",
        "/stats/v1/player/8733",
        "/stats/v1/player/6635",
        "/stats/v1/player/search?plrN=Tucker",
        "/stats/v1/rankings/batsmen?formatType=test",
        "/stats/v1/topstats/0?statsType=mostRuns",
        "/stats/v1/team/2?statsType=mostRuns"
    ]

    players = []
    for url in endpoints:
        try:
            res = requests.get(f"https://cricbuzz-cricket.p.rapidapi.com{url}", headers=HEADERS)
            data = res.json()
            # Try common keys
            for key in ["player", "players", "rank", "values"]:
                if key in data:
                    for entry in data[key]:
                        player = extract_player(entry)
                        if player:
                            players.append(player)
        except Exception as e:
            print(f"❌ Failed to fetch {url}: {e}")
    return players


# --- Main Execution ---
players_data = fetch_players_from_endpoints()
df = pd.DataFrame(players_data).drop_duplicates(subset=["player_id"])

# --- Remove already inserted players ---
try:
    existing_ids = pd.read_sql("SELECT player_id FROM players", con=engine)["player_id"].tolist()
    df = df[~df["player_id"].isin(existing_ids)]
except Exception as e:
    print(f"⚠️ Could not fetch existing player IDs: {e}")

# --- Insert into MySQL ---
if df.empty:
    print("⚠️ No new players to insert.")
else:
    try:
        df.to_sql("players", con=engine, if_exists="append", index=False)
        print(f"✅ Inserted {len(df)} new players into 'players' table.")
    except Exception as e:
        print(f"❌ SQL insert failed: {e}")


#-----------------------------------------------------------------------------------------------
#Insert into Team stats

import requests
import pandas as pd
from sqlalchemy import create_engine

# --- MySQL Connection ---
engine = create_engine("mysql+pymysql://root:Akshiya13@localhost:3306/cricbuzz")

# --- API Configuration ---
url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/international"
headers = {
    "x-rapidapi-key": "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

# --- Fetch Data ---
response = requests.get(url, headers=headers)
if response.status_code != 200:
    raise Exception(f"API request failed: {response.status_code} - {response.text}")

data = response.json()

# --- Parse Team Data ---
teams_data = []
for team in data.get("list", []):
    team_id = team.get("teamId")  # ✅ Correct key
    if not team_id:
        print(f"⚠️ Skipping team with missing ID: {team}")
        continue
    teams_data.append({
        "team_id": int(team_id),
        "team_name": team.get("teamName", ""),
        "country": team.get("countryName", team.get("teamName", ""))  # fallback to name if country missing
    })

df = pd.DataFrame(teams_data).drop_duplicates(subset=["team_id"])

# --- Remove already inserted teams ---
try:
    existing_ids = pd.read_sql("SELECT team_id FROM teams", con=engine)["team_id"].tolist()
    df = df[~df["team_id"].isin(existing_ids)]
except Exception as e:
    print(f"⚠️ Could not fetch existing team IDs: {e}")

# --- Insert into MySQL ---
if df.empty:
    print("⚠️ No new teams to insert.")
else:
    try:
        df.to_sql("teams", con=engine, if_exists="append", index=False)
        print(f"✅ Inserted {len(df)} new teams into 'teams' table.")
    except Exception as e:
        print(f"❌ SQL insert failed: {e}")

#-----------------------------------------------------------------------------------------------
#Insert into Venue & Series stats

import requests
import pandas as pd
from sqlalchemy import create_engine

# --- MySQL Connection ---
engine = create_engine("mysql+pymysql://root:Akshiya13@localhost:3306/cricbuzz")

# --- API Configuration ---
base_url = "https://cricbuzz-cricket.p.rapidapi.com"
headers = {
    "x-rapidapi-key": "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

# --- Collect Venue Data ---
venues_data = []

# 1. From /series/v1/3718/venues
try:
    response = requests.get(f"{base_url}/series/v1/3718/venues", headers=headers)
    if response.status_code == 200:
        data = response.json()
        for venue in data.get("seriesVenue", []):
            venues_data.append({
                "venue_name": venue.get("ground", ""),
                "city": venue.get("city", ""),
                "country": venue.get("country", ""),
                "capacity": None
            })
except Exception as e:
    print(f"⚠️ Error fetching series venues: {e}")

# 2. From /matches/v1/live, /upcoming, /recent
match_endpoints = ["/matches/v1/live", "/matches/v1/upcoming", "/matches/v1/recent"]

for endpoint in match_endpoints:
    try:
        response = requests.get(base_url + endpoint, headers=headers)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {endpoint}: {response.status_code}")
            continue

        data = response.json()
        for match_type in data.get("typeMatches", []):
            for series in match_type.get("seriesMatches", []):
                for match in series.get("matches", []):
                    venue = match.get("venue")
                    if not venue or not venue.get("name"):
                        continue
                    venues_data.append({
                        "venue_name": venue.get("name", ""),
                        "city": venue.get("city", ""),
                        "country": venue.get("country", ""),
                        "capacity": None
                    })
    except Exception as e:
        print(f"⚠️ Error fetching {endpoint}: {e}")

# --- Deduplicate ---
df = pd.DataFrame(venues_data).drop_duplicates(subset=["venue_name", "city", "country"])

# --- Filter out already inserted venues ---
try:
    existing = pd.read_sql("SELECT venue_name, city, country FROM venues", con=engine)
    df = df.merge(existing, on=["venue_name", "city", "country"], how="left", indicator=True)
    df = df[df["_merge"] == "left_only"].drop(columns=["_merge"])
except Exception as e:
    print(f"⚠️ Could not check for existing venues: {e}")

# --- Insert into MySQL ---
if not df.empty:
    try:
        df.to_sql("venues", con=engine, if_exists="append", index=False)
        print(f"✅ Inserted {len(df)} new venues from all API sources.")
    except Exception as e:
        print(f"❌ SQL insert failed: {e}")
else:
    print("⚠️ No new venues to insert.")



import requests
import datetime
import mysql.connector

# --- MySQL Connection ---
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Akshiya13",
    database="cricbuzz"
)
cursor = db.cursor()
print("✅ Connected to MySQL.")

# --- API Configuration ---
base_url = "https://cricbuzz-cricket.p.rapidapi.com"
headers = {
    "x-rapidapi-key": "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}
endpoint = "/matches/v1/recent"

# --- Fetch Recent Matches ---
response = requests.get(base_url + endpoint, headers=headers)
if response.status_code != 200:
    print(f"⚠️ Failed to fetch recent matches: {response.status_code}")
    exit()

data = response.json()
matches = data.get("typeMatches", [])
print(f"📋 Found {len(matches)} match types.")

# --- Helper functions ---
def get_team_id(team_name):
    cursor.execute("SELECT team_id FROM teams WHERE team_name = %s", (team_name,))
    result = cursor.fetchone()
    cursor.fetchall()
    if result:
        return result[0]
    else:
        cursor.execute("SELECT MAX(team_id) FROM teams")
        max_id = cursor.fetchone()[0] or 0
        new_id = max_id + 1
        cursor.execute("""
            INSERT INTO teams (team_id, team_name, country)
            VALUES (%s, %s, %s)
        """, (new_id, team_name, "Unknown"))
        print(f"✅ Inserted new team: {team_name} (ID: {new_id})")
        return new_id

def get_venue_id(venue_name):
    cursor.execute("SELECT venue_id FROM venues WHERE venue_name = %s", (venue_name,))
    result = cursor.fetchone()
    cursor.fetchall()
    if result:
        return result[0]
    else:
        cursor.execute("SELECT MAX(venue_id) FROM venues")
        max_id = cursor.fetchone()[0] or 0
        new_id = max_id + 1
        cursor.execute("""
            INSERT INTO venues (venue_id, venue_name, city, country, capacity)
            VALUES (%s, %s, %s, %s, %s)
        """, (new_id, venue_name, "Unknown", "Unknown", 0))
        print(f"✅ Inserted new venue: {venue_name} (ID: {new_id})")
        return new_id

# --- Parse and Insert Matches ---
for match_type in matches:
    for series in match_type.get("seriesMatches", []):
        for match in series.get("seriesAdWrapper", {}).get("matches", []):
            info = match.get("matchInfo", {})
            if not info:
                continue

            match_id = int(info.get("matchId", 0))
            match_desc = info.get("matchDesc", "")
            match_date = datetime.datetime.fromtimestamp(int(info.get("startDate", 0)) / 1000).date()
            match_status = info.get("status", "")
            team1_name = info.get("team1", {}).get("teamName", "")
            team2_name = info.get("team2", {}).get("teamName", "")
            toss_winner = info.get("tossResults", {}).get("tossWinner", "")
            toss_decision = info.get("tossResults", {}).get("decision", "")
            winning_team = info.get("matchWinner", "")
            venue_name = info.get("venueInfo", {}).get("ground", "")

            team1_id = get_team_id(team1_name)
            team2_id = get_team_id(team2_name)
            venue_id = get_venue_id(venue_name)

            margin = info.get("matchScore", {}).get("margin", {})
            victory_margin = margin.get("value", None)
            victory_type = margin.get("type", None)

            try:
                cursor.execute("""
                    INSERT INTO matches (
                        match_id, match_description, match_date, match_status,
                        team1_id, team2_id, winning_team, victory_margin, victory_type,
                        venue_id, toss_winner, toss_decision
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    match_id, match_desc, match_date, match_status,
                    team1_id, team2_id, winning_team, victory_margin, victory_type,
                    venue_id, toss_winner, toss_decision
                ))
                print(f"✅ Inserted match {match_id}")
            except mysql.connector.Error as err:
                print(f"❌ SQL Error for match {match_id}: {err}")

db.commit()
cursor.close()
db.close()
print("✅ All match records inserted and connection closed.")


#-----------------------------------------------------------------------------------------------
#Insert into Batting stats

import http.client
import json
import mysql.connector

# --- MySQL Connection ---
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Akshiya13",
    database="cricbuzz"
)
cursor = db.cursor()
print("✅ Connected to MySQL.")

# --- API Setup ---
conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")
headers = {
    'x-rapidapi-key': "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}

# --- Match IDs to process ---
match_ids = [
    116954, 123738, 123754, 124370, 124403, 124414, 124425, 124447, 124475,
    124480, 124491, 124497, 133253, 133259, 135560, 135611, 135622, 135859,
    136291, 136326, 137481
]

# --- Helper: Insert player if missing ---
def ensure_player(player_id, name):
    cursor.execute("SELECT player_id FROM players WHERE player_id = %s", (player_id,))
    result = cursor.fetchone()
    cursor.fetchall()
    if not result:
        cursor.execute("""
            INSERT INTO players (player_id, full_name, country, playing_role, batting_style, bowling_style)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (player_id, name, "Unknown", "Unknown", "Unknown", "Unknown"))
        print(f"✅ Inserted player: {name} (ID: {player_id})")

# --- Loop through match IDs ---
for match_id in match_ids:
    print(f"📥 Fetching scorecard for match {match_id}")
    conn.request("GET", f"/mcenter/v1/{match_id}/hscard", headers=headers)
    res = conn.getresponse()
    data = res.read()

    try:
        scorecard = json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"❌ Failed to decode JSON for match {match_id}: {e}")
        continue

    innings_list = scorecard.get("scorecard", [])
    if not isinstance(innings_list, list):
        print(f"⚠️ Unexpected scorecard format for match {match_id}")
        continue

    for innings in innings_list:
        batsmen = innings.get("batsman", [])
        if not isinstance(batsmen, list):
            continue

        for position, batter in enumerate(batsmen, start=1):
            player_id = int(batter.get("id"))
            name = batter.get("name", "Unknown")
            runs = int(batter.get("runs", 0))
            balls = int(batter.get("balls", 0))
            strike_rate = float(batter.get("strkrate", "0").replace(",", ""))
            is_out = batter.get("outdec", "").strip() != ""

            ensure_player(player_id, name)

            cursor.execute("""
                INSERT INTO batting_stats (
                    player_id, match_id, format, runs_scored, balls_faced,
                    strike_rate, batting_position, is_out
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (player_id, match_id, "T20I", runs, balls, strike_rate, position, is_out))
            print(f"✅ Inserted batting stats for {name} (ID: {player_id}) in match {match_id}")

db.commit()
cursor.close()
db.close()
print("✅ All records inserted and connection closed.")


#-----------------------------------------------------------------------------------------------
#Insert into bowler stats

import http.client
import json
import mysql.connector

# --- MySQL Connection ---
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Akshiya13",
    database="cricbuzz"
)
cursor = db.cursor()
print("✅ Connected to MySQL.")

# --- API Setup ---
conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")
headers = {
    'x-rapidapi-key': "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}

# --- Match IDs to process ---
match_ids = [
    116954, 123738, 123754, 124370, 124403, 124414, 124425, 124447, 124475,
    124480, 124491, 124497, 133253, 133259, 135560, 135611, 135622, 135859,
    136291, 136326, 137481
]

# --- Helper: Insert player if missing ---
def ensure_player(player_id, name):
    cursor.execute("SELECT player_id FROM players WHERE player_id = %s", (player_id,))
    result = cursor.fetchone()
    cursor.fetchall()
    if not result:
        cursor.execute("""
            INSERT INTO players (player_id, full_name, country, playing_role, batting_style, bowling_style)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (player_id, name, "Unknown", "Unknown", "Unknown", "Unknown"))
        print(f"✅ Inserted player: {name} (ID: {player_id})")

# --- Loop through match IDs ---
for match_id in match_ids:
    print(f"📥 Fetching scorecard for match {match_id}")
    conn.request("GET", f"/mcenter/v1/{match_id}/hscard", headers=headers)
    res = conn.getresponse()
    data = res.read()

    try:
        scorecard = json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"❌ Failed to decode JSON for match {match_id}: {e}")
        continue

    innings_list = scorecard.get("scorecard", [])
    if not isinstance(innings_list, list):
        print(f"⚠️ Unexpected scorecard format for match {match_id}")
        continue

    for innings in innings_list:
        bowlers = innings.get("bowler", [])
        if not isinstance(bowlers, list):
            continue

        for bowler in bowlers:
            player_id = int(bowler.get("id"))
            name = bowler.get("name", "Unknown")
            overs = float(bowler.get("overs", "0").replace(",", ""))
            runs = int(bowler.get("runs", 0))
            wickets = int(bowler.get("wickets", 0))
            economy = float(bowler.get("econ", "0").replace(",", ""))
            match_format = "T20I"

            ensure_player(player_id, name)

            cursor.execute("""
                INSERT INTO bowling_stats (
                    player_id, match_id, format, overs_bowled,
                    runs_conceded, wickets_taken, economy_rate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (player_id, match_id, match_format, overs, runs, wickets, economy))
            print(f"✅ Inserted bowling stats for {name} (ID: {player_id}) in match {match_id}")

db.commit()
cursor.close()
db.close()
print("✅ All records inserted and connection closed.")

#----------------------------------------------------------------------------------
# Insertion into Player_match

import http.client
import json
import mysql.connector
from datetime import datetime

# Connect to Cricbuzz API
conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")
headers = {
    'x-rapidapi-key': "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}
conn.request("GET", "/mcenter/v1/41881/overs", headers=headers)
res = conn.getresponse()
raw_data = res.read()
data = json.loads(raw_data.decode("utf-8"))

import requests
import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(user='root', password='Akshiya13', database='cricbuzz')
cursor = conn.cursor()

headers = {
    'x-rapidapi-key': "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}

player_ids = [576, 587, 673, 674, 866, 1413, 1447, ...]  # Truncated for brevity

for pid in player_ids:
    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        continue  # Skip if API fails

    data = response.json()
    matches = data.get("matchBattingStatsList", [])  # Adjust key based on actual response

    for match in matches:
        match_date = match.get("matchDate")
        runs = match.get("runs")
        balls = match.get("ballsFaced")
        if not match_date or runs is None or balls is None or balls == 0:
            continue

        strike_rate = round((runs / balls) * 100, 2)

        cursor.execute("""
            INSERT INTO player_match (player_id, match_date, runs_scored, strike_rate)
            VALUES (%s, %s, %s, %s)
        """, (pid, match_date, runs, strike_rate))

conn.commit()

#--------------------------------------------------------------------------------------------------
import http.client
import json
import mysql.connector
from datetime import datetime

# Connect to Cricbuzz API
conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")
headers = {
    'x-rapidapi-key': "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}
conn.request("GET", "/mcenter/v1/41881/overs", headers=headers)
res = conn.getresponse()
raw_data = res.read()
data = json.loads(raw_data.decode("utf-8"))

import requests
import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(user='root', password='Akshiya13', database='cricbuzz')
cursor = conn.cursor()

headers = {
    'x-rapidapi-key': "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192",
    'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
}

player_ids = [576, 587, 673, 674, 866, 1413, 1447]

for pid in player_ids:
    url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{pid}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        continue  # Skip if API fails

    data = response.json()
    matches = data.get("matchBattingStatsList", [])  # Adjust key based on actual response

    for match in matches:
        match_date = match.get("matchDate")
        runs = match.get("runs")
        balls = match.get("ballsFaced")
        if not match_date or runs is None or balls is None or balls == 0:
            continue

        strike_rate = round((runs / balls) * 100, 2)

        cursor.execute("""
            INSERT INTO player_match (player_id, match_date, runs_scored, strike_rate)
            VALUES (%s, %s, %s, %s)
        """, (pid, match_date, runs, strike_rate))

conn.commit()

#------------------------------------------------------------------------------------------
import requests
import mysql.connector
from datetime import datetime
import time

# --- CONFIG ---
API_KEY = "302155078cmsh2208ec1f59588b9p1f519ajsn39be0946a192"
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Akshiya13',
    'database': 'cricbuzz'
}

# --- DB CONNECTION ---
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

# --- FETCH SCORECARD ---
def fetch_scorecard(match_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}/scard"
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"[{match_id}] HTTP Error {response.status_code}")
            return None
        return response.json()
    except Exception as e:
        print(f"[{match_id}] Fetch error: {e}")
        return None

# --- PLAYER RECENT FORM ---
def extract_recent_form(scorecard, match_id):
    data = []
    if not scorecard or 'matchHeader' not in scorecard or 'innings' not in scorecard:
        return data
    match_date = datetime.fromtimestamp(scorecard['matchHeader']['startDate'] / 1000).date()
    for innings in scorecard['innings']:
        for player in innings.get('batting', {}).get('batters', []):
            player_id = int(player['id'])
            runs = int(player.get('runs', 0))
            balls = int(player.get('balls', 0))
            sr = float(player.get('strikeRate', 0.0))
            is_above_50 = runs >= 50
            data.append((player_id, match_id, runs, balls, sr, is_above_50, match_date))
    return data

def insert_recent_form(data):
    if not data: return
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO player_recent_form 
        (player_id, match_id, runs_scored, balls_faced, strike_rate, is_above_50, match_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

# --- FIELDING STATS ---
def extract_fielding_stats(scorecard, match_id):
    data = []
    for innings in scorecard.get('innings', []):
        for player in innings.get('fielding', {}).get('players', []):
            player_id = int(player['id'])
            catches = int(player.get('catches', 0))
            stumpings = int(player.get('stumpings', 0))
            runouts = int(player.get('runouts', 0))
            data.append((player_id, match_id, catches, stumpings, runouts))
    return data

def insert_fielding_stats(data):
    if not data: return
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO fielding_stats 
        (player_id, match_id, catches, stumpings, runouts)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

# --- BATTING PARTNERSHIPS ---
def extract_partnerships(scorecard, match_id):
    data = []
    for innings in scorecard.get('innings', []):
        partnerships = innings.get('partnerships', [])
        for p in partnerships:
            p1 = int(p['batsmen'][0]['id'])
            p2 = int(p['batsmen'][1]['id'])
            pos1 = int(p['batsmen'][0]['position'])
            pos2 = int(p['batsmen'][1]['position'])
            runs = int(p['runs'])
            innings_num = int(innings['inningsId'])
            data.append((match_id, innings_num, p1, p2, pos1, pos2, runs))
    return data

def insert_partnerships(data):
    if not data: return
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO batting_partnerships 
        (match_id, innings_number, player1_id, player2_id, batting_position1, batting_position2, partnership_runs)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

# --- FORMAT SUMMARY ---
def fetch_player_stats(player_id):
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }

    bat_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/batting"
    bowl_url = f"https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{player_id}/bowling"

    try:
        bat_resp = requests.get(bat_url, headers=headers)
        bowl_resp = requests.get(bowl_url, headers=headers)

        bat_json = bat_resp.json()
        bowl_json = bowl_resp.json()

        bat = bat_json.get("stats", [])
        bowl = bowl_json.get("stats", [])

        if not isinstance(bat, list):
            print(f"[{player_id}] Batting stats not in expected format: {type(bat)}")
            bat = []

        if not isinstance(bowl, list):
            print(f"[{player_id}] Bowling stats not in expected format: {type(bowl)}")
            bowl = []

        return bat, bowl

    except Exception as e:
        print(f"[{player_id}] Error fetching player stats: {e}")
        return [], []

def extract_format_summary(player_id, bat, bowl):
    data = []

    # Convert batting list to dict by format
    bat_by_format = {item['format']: item for item in bat if 'format' in item}
    bowl_by_format = {item['format']: item for item in bowl if 'format' in item}

    formats = set(bat_by_format.keys()) | set(bowl_by_format.keys())

    for fmt in formats:
        b = bat_by_format.get(fmt, {})
        bo = bowl_by_format.get(fmt, {})
        row = (
            player_id, fmt,
            int(b.get('runs', 0)),
            int(bo.get('wickets', 0)),
            float(b.get('average', 0)),
            float(bo.get('average', 0)),
            float(b.get('strikeRate', 0)),
            float(bo.get('economyRate', 0)),
            int(b.get('hundreds', 0)),
            int(b.get('matches', 0)),
            int(b.get('innings', 0))
        )
        data.append(row)
    return data

def insert_format_summary(data):
    if not data: return
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO player_format_summary 
        (player_id, format, total_runs, total_wickets, batting_average, bowling_average, strike_rate, economy_rate, centuries, matches_played, innings_played)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

# --- QUARTERLY STATS ---
def extract_quarterly_stats(player_id, bat):
    data = []
    for record in bat:
        if not isinstance(record, dict):
            continue
        fmt = record.get('format')
        timeline = record.get('timeline', [])
        for r in timeline:
            if not isinstance(r, dict):
                continue
            year = int(r.get('year', 0))
            month = int(r.get('month', 1))
            quarter = (month - 1) // 3 + 1
            runs = int(r.get('runs', 0))
            sr = float(r.get('strikeRate', 0.0))
            matches = int(r.get('matches', 0))
            data.append((player_id, year, quarter, fmt, runs, sr, matches))
    return data

def insert_quarterly_stats(data):
    if not data: return
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """
        INSERT INTO player_quarterly_stats 
        (player_id, year, quarter, format, total_runs, strike_rate, matches_played)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

# --- MASTER INGESTION ---
def ingest_all(match_id, player_ids):
    scorecard = fetch_scorecard(match_id)
    if scorecard:
        insert_recent_form(extract_recent_form(scorecard, match_id))
        insert_fielding_stats(extract_fielding_stats(scorecard, match_id))
        insert_partnerships(extract_partnerships(scorecard, match_id))
        print(f"[{match_id}] Match data inserted.")
    else:
        print(f"[{match_id}] Scorecard fetch failed.")

    for pid in player_ids:
        bat, bowl = fetch_player_stats(pid)
        insert_format_summary(extract_format_summary(pid, bat, bowl))
        insert_quarterly_stats(extract_quarterly_stats(pid, bat))
        print(f"[{pid}] Player stats inserted.")

# --- EXECUTION ---
match_ids = [40381, 41881, 35878]
player_ids = [8733, 15826]  # Add more player IDs as needed

for mid in match_ids:
    ingest_all(mid, player_ids)
    time.sleep(1.5)