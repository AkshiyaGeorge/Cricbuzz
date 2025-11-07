import streamlit as st
import requests
from datetime import datetime
import pymysql
import pandas as pd
import mysql.connector
import http.client
import json


# -------------------- API Config --------------------
BASE_HEADERS = {
    "x-rapidapi-key": "1c45c8492amsh7fe28c54551ccf2p19621djsnefd72a76eeac",
    "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

ENDPOINTS = {
    "matches": "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/current",
    "player_search": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/search?plrN={player_name}",
    "match_players": "https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}",
    "rankings": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen?formatType={format_type}",
    "search": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/search?plrN={name}",
    "batting": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{id}/batting",
    "bowling": "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/{id}/bowling",
    "news": "https://cricbuzz-cricket.p.rapidapi.com/news/v1/player/{id}"
}

# -------------------------Defining functions --------------------------------
def fetch_players(match_id):
    url = f"https://cricbuzz-cricket.p.rapidapi.com/mcenter/v1/{match_id}"
    headers = {
        "X-RapidAPI-Key": "1c45c8492amsh7fe28c54551ccf2p19621djsnefd72a76eeac",
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        team_players = {"playing XI": [], "bench": []}
        for team in data.get("team", []):
            for player in team.get("players", []):
                role = player.get("playingRole", "").lower()
                name = player.get("name", "Unknown")
                if "xi" in role:
                    team_players["playing XI"].append(name)
                elif "bench" in role:
                    team_players["bench"].append(name)
        return team_players
    return None

def format_player(name):
    return f"**{name}**"


import http.client
import json

def get_trending_players():
    import http.client
    import json

    conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': "1c45c8492amsh7fe28c54551ccf2p19621djsnefd72a76eeac",
        'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
    }

    try:
        conn.request("GET", "/stats/v1/player/trending", headers=headers)
        res = conn.getresponse()
        status = res.status
        data = res.read()

        # ✅ Log status code and raw response
        st.write(f"API Status Code: {status}")
        st.write("Raw API Response:", data.decode("utf-8"))

        if status != 200:
            st.error("❌ Failed to fetch trending players.")
            return []

        response_json = json.loads(data.decode("utf-8"))
        st.write("Parsed JSON:", response_json)

        # ✅ Try multiple possible keys
        for key in ["playerList", "players", "list"]:
            if key in response_json:
                st.success(f"✅ Found player data under key: '{key}'")
                return response_json[key]

        st.warning("⚠️ No player data found in expected keys.")
        return []

    except Exception as e:
        st.error(f"🚨 Error fetching trending players: {e}")
        return []

# -------------------- Players API Calls --------------------

def search_player(name):
    url = ENDPOINTS["search"].format(name=name)
    res = requests.get(url, headers=BASE_HEADERS)
    return res.json().get("player", []) if res.status_code == 200 else []

def fetch_matrix_stats(player_id, stat_type):
    url = ENDPOINTS[stat_type].format(id=player_id)
    res = requests.get(url, headers=BASE_HEADERS)
    data = res.json()
    headers = data.get("headers", [])
    rows = [v["values"] for v in data.get("values", [])]
    return pd.DataFrame(rows, columns=headers) if headers and rows else pd.DataFrame()

def fetch_news(player_id):
    url = ENDPOINTS["news"].format(id=player_id)
    res = requests.get(url, headers=BASE_HEADERS)
    return res.json().get("storyList", []) if res.status_code == 200 else []

def format_time(ts):
    try:
        return datetime.fromtimestamp(int(ts) / 1000).strftime("%d %b %Y")
    except:
        return "–"


def convert_ts(ts):
    return datetime.fromtimestamp(int(ts)/1000).strftime("%d %b %Y %I:%M %p") if ts else "N/A"


# -------------------- Live match API Calls --------------------
@st.cache_data(ttl=300)
def fetch_live_matches():
    response = requests.get(ENDPOINTS["matches"], headers=BASE_HEADERS)
    if response.status_code != 200:
        return None, "❌ Failed to fetch match data."

    data = response.json()
    matches = []

    for matchtype in data.get("typeMatches", []):
        for seriesmatch in matchtype.get("seriesMatches", []):
            series_wrapper = seriesmatch.get("seriesAdWrapper", {})
            series_name = series_wrapper.get("seriesName")

            for match in series_wrapper.get("matches", []):
                info = match.get("matchInfo", {})
                venue = info.get("venueInfo", {})
                score = info.get("matchScore", {})
                team1 = info.get("team1", {})
                team2 = info.get("team2", {})
                team1_score = score.get("team1Score", {}).get("inngs1", {})
                team2_score = score.get("team2Score", {}).get("inngs1", {})

                matches.append({
                    "label": f"{team1.get('teamName')} vs {team2.get('teamName')} ({series_name})",
                    "Series": info.get("seriesName"),
                    "Match": f"{team1.get('teamName')} vs {team2.get('teamName')}",
                    "Description": info.get("matchDesc"),
                    "Format": info.get("matchFormat"),
                    "Status": info.get("status"),
                    "Result": info.get("stateTitle"),
                    "Venue": f"{venue.get('ground')}, {venue.get('city')}",
                    "Start Date": convert_ts(info.get("startDate")),
                    "Team 1 Score": f"{team1_score.get('runs', '–')}/{team1_score.get('wickets', '–')} in {team1_score.get('overs', '–')} overs",
                    "Team 2 Score": f"{team2_score.get('runs', '–')}/{team2_score.get('wickets', '–')} in {team2_score.get('overs', '–')} overs",
                    "matchId": info.get("matchId")
                })
    return matches, None

@st.cache_data(ttl=300)
def fetch_players(match_id):
    url = ENDPOINTS["match_players"].format(match_id=match_id)
    response = requests.get(url, headers=BASE_HEADERS)
    if response.status_code != 200:
        return None
    return response.json().get("players", {})

@st.cache_data(ttl=300)
def search_players(name):
    url = ENDPOINTS["player_search"].format(player_name=name)
    response = requests.get(url, headers=BASE_HEADERS)
    if response.status_code != 200:
        return None
    return response.json().get("player", [])

@st.cache_data(ttl=300)
def fetch_rankings(format_type):
    url = ENDPOINTS["rankings"].format(format_type=format_type)
    response = requests.get(url, headers=BASE_HEADERS)
    if response.status_code != 200:
        return None
    return response.json().get("rank", [])

@st.cache_data(ttl=600)
def fetch_all_players():
    url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/player/search?plrN="
    headers = {
        'x-rapidapi-key': "302155078cmsh7fe28c54551ccf2p19621djsnefd72a76eeac",
        'x-rapidapi-host': "cricbuzz-cricket.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("player", [])
    else:
        st.error("🧙‍♀️ Type a name, hit enter, and let the stats wizard conjure up cricket greatness!")
        return []

# -------------------- Streamlit UI --------------------
# Page configuration
st.set_page_config(page_title="CricBuzz Dashboard", layout="wide")
st.markdown("""
    <style>
        body {
            background-color: #fffde7; /* Pale yellow */
        }
        .main {
            padding-top: 10px;
        }
        .header h1 {
            font-size: 3.2em;
            color: #0d47a1; /* Dark blue */
            text-align: center;
            margin-bottom: 5px;
        }
        .divider {
            border-top: 2px solid #e0e0e0;
            margin: 10px 0 30px 0;
        }
        .welcome-text {
            text-align: center;
            font-size: 1.3em;
            color: #333333;
            margin-bottom: 30px;
        }
        .footer {
            text-align: center;
            font-size: 0.9em;
            color: #5c6bc0;
            margin-top: 40px;
        }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown("<div class='header'><h1>🏏 Cricbuzz Dashboard 🏏</h1></div>", unsafe_allow_html=True)

# Divider
st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

# Welcome Text
st.markdown("""
    <div class='welcome-text'>
        <b><i>Welcome to the Cricbuzz Dashboard! <i><b><br><br>
        Explore 📡 <b>Live Match Info</b>, 👨🏻 <b>Player Profiles</b>, ⭐ <b>ICC Rankings</b>, 📈 <b>SQL Analysis</b> and ⚙️ <b>CURD Operations</b> using the sidebar.
    </div>
""", unsafe_allow_html=True)


# Sidebar Header
st.sidebar.markdown("### 🏠 Home Page \n### Cricket info Sections 🏏")

# Section Selector
selected_section = st.sidebar.selectbox(
    "Choose a section to view",
    ["Make your selection", "📡Live Match Info", "👨🏻Players Stats", "⭐ICC Batsmen Rankings","📈SQL Analysis", "⚙️CURD Operations"]
)

# ⭐ ICC Batsmen Rankings
if selected_section == "⭐ICC Batsmen Rankings":
    format_map = {"Test": "test", "ODI": "odi", "T20": "t20"}
    selected_format = st.sidebar.selectbox("Select Format", list(format_map.keys()), key="icc_format")
    format_type = format_map[selected_format]

    st.markdown("## 🏅 ICC Batsmen Rankings")
    top_batsmen = fetch_rankings(format_type)
    if top_batsmen:
        with st.expander(f"🧮 Top {len(top_batsmen)} {selected_format} Batsmen"):
            for player in top_batsmen[:15]:
                st.markdown(f"- 🏏 **{player['name']}** ({player['country']}) 🌍 — Rating: {player['rating']} ⭐")

# 📡 Live Match Info
elif selected_section == "📡Live Match Info":
    st.markdown("## 📺 Live Match Info")
    matches, error = fetch_live_matches()
    if error:
        st.error(error)
    elif not matches:
        st.warning("No live matches found.")
    else:
        match_labels = [m["label"] for m in matches]
        selected_label = st.selectbox("Select a Match", match_labels)
        selected_match = next((m for m in matches if m["label"] == selected_label), None)

        st.markdown(f"### 📊 Match Details: {selected_match['Match']}")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Series:** {selected_match['Series']}")
            st.markdown(f"**Description:** {selected_match['Description']}")
            st.markdown(f"**Format:** {selected_match['Format']}")
            st.markdown(f"**Venue:** {selected_match['Venue']}")
            st.markdown(f"**Start Date:** {selected_match['Start Date']}")
        with col2:
            st.markdown(f"**Status:** {selected_match['Status']}")
            st.markdown(f"**Result:** {selected_match['Result']}")
            st.markdown(f"**Team 1 Score:** {selected_match['Team 1 Score']}")
            st.markdown(f"**Team 2 Score:** {selected_match['Team 2 Score']}")

        player_data = fetch_players(selected_match["matchId"])

        if player_data and ("playing XI" in player_data or "bench" in player_data):
            if "playing XI" in player_data and player_data["playing XI"]:
                with st.expander("👥 Playing XI"):
                    for player in player_data["playing XI"]:
                        st.markdown(f"- {format_player(player)}")
            if "bench" in player_data and player_data["bench"]:
                with st.expander("🪑 Bench Players"):
                    for player in player_data["bench"]:
                        st.markdown(f"- {format_player(player)}")
        else:
            st.info(
                "🔄 Player info is not available for this match. It may be missing from the API or not yet published.")


# 👨🏻 Players Stats + Batting + Bowling

elif selected_section == "👨🏻Players Stats":
    st.markdown("## 🔥 Players Stats")

    # 🔍 Sidebar Search Input
    search_name = st.sidebar.text_input("🔍 Search Player by Name")

    # 🔽 Dropdown for Matching Players
    matched_players = search_players(search_name) if search_name else fetch_all_players()
    player_options = [f"{p['name']} ({p['teamName']})" for p in matched_players] if matched_players else []
    selected_label = st.sidebar.selectbox("🎯 Select Player", player_options) if player_options else None

    # 👤 Show Selected Player Info
    if selected_label:
        selected_player = next(
            (p for p in matched_players if f"{p['name']} ({p['teamName']})" == selected_label),
            {}
        )
        player_id = selected_player.get("id")
        team_name = selected_player.get("teamName", "Unknown")
        name = selected_player.get("name", "Unknown")

        st.markdown(f"### 📋 Player Info: {name}")
        st.markdown(f"#### {name} ({team_name})")

        # 📊 Stats + News Tabs
        tab1, tab2, tab3 = st.tabs(["🏏 Batting", "🎯 Bowling", "📰 News"])

        with tab1:
            st.subheader("Batting Stats")
            bat_df = fetch_matrix_stats(player_id, "batting")
            if not bat_df.empty:
                st.dataframe(bat_df, use_container_width=True)
            else:
                st.info("No batting stats available.")

        with tab2:
            st.subheader("Bowling Stats")
            bowl_df = fetch_matrix_stats(player_id, "bowling")
            if not bowl_df.empty:
                st.dataframe(bowl_df, use_container_width=True)
            else:
                st.info("No bowling stats available.")

        with tab3:
            st.subheader("Latest News")
            news_items = fetch_news(player_id)
            stories = [item.get("story") for item in news_items if item.get("story")]
            if stories:
                for story in stories:
                    st.markdown(f"**🗞️ {story.get('hline')}**")
                    st.caption(f"{story.get('context', '')} • {format_time(story.get('pubTime'))}")
                    st.write(story.get("intro", ""))
                    caption = story.get("coverImage", {}).get("caption")
                    if caption:
                        st.markdown(f"📸 *{caption}*")
                    st.markdown("---")
            else:
                st.info("No news available for this player.")
    else:
        st.info("Select a player from the dropdown to view stats.")


# 🗃️ Manage Local Player Database
elif selected_section == "⚙️CURD Operations":
    # Connect to SQL
    conn = pymysql.connect(
        host="localhost",
        user="root",
        password="Akshiya13",
        database="cricbuzz"
    )
    cursor = conn.cursor()

    # Header
    st.header("🗃️ Manage Local Player Database")

    # Dropdown for CRUD actions
    crud_action = st.selectbox("🛠️ CURD Operations", [
        "➕ Add Player",
        "📋 Display All Players",
        "✏️ Update Player Team",
        "🗑️ Delete Player"
    ])

    # ➕ Add Player
    import traceback

    if crud_action == "➕ Add Player":
        st.subheader("➕ Add Player")

        # Input fields
        player_id = st.number_input("Player ID", min_value=1)
        full_name = st.text_input("Player Name")
        country = st.text_input("Team")

        # Debug: show entered values
        st.write(f"🔍 Debug: ID={player_id}, Name='{full_name}', Country='{country}'")

        # Add Player button
        if st.button("Add Player"):
            if player_id > 0 and full_name.strip() and country.strip():
                try:
                    cursor.execute(
                        "INSERT INTO players (player_id, full_name, country) VALUES (%s, %s, %s)",
                        (player_id, full_name.strip(), country.strip())
                    )
                    conn.commit()
                    st.success(f"✅ Player '{full_name}' added to team '{country}' with ID {player_id}")
                except Exception as e:
                    st.error(f"❌ Database error: {e}")
                    st.text(traceback.format_exc())
            else:
                st.warning("⚠️ Please enter Player ID, name and team.")

    # 📋 Display All Players
    elif crud_action == "📋 Display All Players":
        st.subheader("📋 View All Players")

        try:
            cursor.execute("SELECT * FROM players")
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=columns)

            df.rename(columns={
                "player_id": "ID",
                "full_name": "Name",
                "country": "Team",
                "playing_role": "Role",
                "batting_style": "Batting Style",
                "bowling_style": "Bowling Style"
            }, inplace=True)

            if df.empty:
                st.info("ℹ️ No player data found.")
            else:
                st.dataframe(df)

        except Exception as e:
            st.error(f"❌ Error fetching players: {e}")
            import traceback

            st.text(traceback.format_exc())

    # ✏️ Update Player Team
    elif crud_action == "✏️ Update Player Team":
        st.subheader("✏️ Update Player Team")
        player_id = st.number_input("Player ID to update", min_value=1)
        new_team = st.text_input("New Team")
        if st.button("Update Team!"):
            try:
                cursor.execute("UPDATE players SET country = %s WHERE player_id = %s", (new_team, player_id))
                conn.commit()
                st.success("✅ Team updated!")
            except Exception as e:
                st.error(f"❌ Update failed: {e}")

    # 🗑️ Delete Player
    elif crud_action == "🗑️ Delete Player":
        st.subheader("🗑️ Delete Player")
        delete_id = st.number_input("Player ID to delete", min_value=1)
        if st.button("Delete Player"):
            try:
                cursor.execute("DELETE FROM players WHERE player_id = %s", (delete_id,))
                conn.commit()
                st.warning("🗑️ Player deleted.")
            except Exception as e:
                st.error(f"❌ Deletion failed: {e}")



# 📈 SQL Analysis
queries = {
    "1.Players from India": """
                            SELECT full_name, playing_role, batting_style, bowling_style
                            FROM players
                            WHERE country = 'India';
                            """,
    "2.Matches played in the last 30 days":
                                        """SELECT m.match_description, t1.team_name AS team1, t2.team_name AS team2, v.venue_name, v.city, m.match_date
                                            FROM matches m
                                            JOIN teams t1 ON m.team1_id = t1.team_id
                                            JOIN teams t2 ON m.team2_id = t2.team_id
                                            JOIN venues v ON m.venue_id = v.venue_id
                                            WHERE m.match_date >= CURDATE() - INTERVAL 30 DAY
                                            ORDER BY m.match_date DESC; 
                                        """,
    "3.Top 10 ODI run scorers":
                                """SELECT p.full_name, pfs.total_runs, pfs.batting_average, pfs.centuries
                                    FROM player_format_summary pfs
                                    JOIN players p ON pfs.player_id = p.player_id
                                    WHERE pfs.format = 'ODI'
                                    ORDER BY pfs.total_runs DESC
                                    LIMIT 10; 
                                """,
    "4.Venues with capacity > 50,000":"""SELECT venue_name, city, country, capacity
                                            FROM venues
                                            WHERE capacity > 50000
                                            ORDER BY capacity DESC;
                                      """,

    "5.Matches won by each team":
                                    """ SELECT t.team_name, COUNT(*) AS total_wins
                                        FROM matches m
                                        JOIN teams t ON m.winning_team = t.team_name
                                        WHERE m.match_status = 'Completed'
                                        GROUP BY t.team_name
                                        ORDER BY total_wins DESC;
                                        """,

    "6.Count of players by playing role":
                                            """SELECT playing_role, COUNT(*) AS player_count
                                                FROM players
                                                GROUP BY playing_role;
                                                """,

    "7.Highest individual score by format": """
                                            SELECT format, MAX(runs_scored) AS highest_score
                                            FROM batting_stats
                                            GROUP BY format;
                                            """,

    "8.Series started in 2024": """
                                SELECT series_name, host_country, match_type, start_date, total_matches
                                FROM series
                                WHERE YEAR(start_date) = 2024;
                                """,

    "9.All-rounders with 1000+ runs and 50+ wickets": """
                                                      SELECT p.full_name, pfs.format, pfs.total_runs, pfs.total_wickets
                                                      FROM player_format_summary pfs
                                                               JOIN players p ON pfs.player_id = p.player_id
                                                      WHERE p.playing_role = 'All-rounder'
                                                        AND pfs.total_runs > 1000
                                                        AND pfs.total_wickets > 50;
                                                      """,

    "10.Last 20 completed matches": """
                                    SELECT m.match_description, t1.team_name AS team1, t2.team_name AS team2,
                                           m.winning_team, m.victory_margin, m.victory_type,
                                           v.venue_name, m.match_date
                                    FROM matches m
                                             JOIN teams t1 ON m.team1_id = t1.team_id
                                             JOIN teams t2 ON m.team2_id = t2.team_id
                                             JOIN venues v ON m.venue_id = v.venue_id
                                    WHERE m.match_status = 'Completed'
                                    ORDER BY m.match_date DESC
                                        LIMIT 20;
                                    """,

    "11.Player performance across formats": """
                                            SELECT p.full_name,
                                                   MAX(CASE WHEN pfs.format = 'Test' THEN pfs.total_runs ELSE 0 END) AS test_runs,
                                                   MAX(CASE WHEN pfs.format = 'ODI' THEN pfs.total_runs ELSE 0 END) AS odi_runs,
                                                   MAX(CASE WHEN pfs.format = 'T20I' THEN pfs.total_runs ELSE 0 END) AS t20_runs,
                                                   ROUND(AVG(pfs.batting_average), 2) AS overall_batting_avg
                                            FROM player_format_summary pfs
                                                     JOIN players p ON pfs.player_id = p.player_id
                                            GROUP BY p.player_id
                                            HAVING COUNT(DISTINCT pfs.format) >= 2;
                                            """,

    "12.Team wins at home vs away": """
                                    SELECT t.team_name,
                                           SUM(CASE WHEN t.country = v.country THEN 1 ELSE 0 END) AS home_wins,
                                           SUM(CASE WHEN t.country != v.country THEN 1 ELSE 0 END) AS away_wins
                                    FROM matches m
                                             JOIN teams t ON m.winning_team = t.team_name
                                             JOIN venues v ON m.venue_id = v.venue_id
                                    WHERE m.match_status = 'Completed'
                                    GROUP BY t.team_name;
                                    """,

    "13.Consecutive batsmen with 100+ partnership": """
                                                    SELECT p1.full_name AS batsman1, p2.full_name AS batsman2,
                                                           bp.partnership_runs, bp.innings_number
                                                    FROM batting_partnerships bp
                                                             JOIN players p1 ON bp.player1_id = p1.player_id
                                                             JOIN players p2 ON bp.player2_id = p2.player_id
                                                    WHERE ABS(bp.batting_position1 - bp.batting_position2) = 1
                                                      AND bp.partnership_runs >= 100;
                                                    """,

    "14.Bowling performance at venues (≥3 matches, ≥4 overs)": """
                                                               SELECT p.full_name, v.venue_name,
                                                                      COUNT(*) AS matches_played,
                                                                      SUM(bs.wickets_taken) AS total_wickets,
                                                                      ROUND(AVG(bs.economy_rate), 2) AS avg_economy
                                                               FROM bowling_stats bs
                                                                        JOIN players p ON bs.player_id = p.player_id
                                                                        JOIN matches m ON bs.match_id = m.match_id
                                                                        JOIN venues v ON m.venue_id = v.venue_id
                                                               WHERE bs.overs_bowled >= 4.0
                                                               GROUP BY p.player_id, v.venue_id
                                                               HAVING COUNT(*) >= 3;
                                                               """,

    "15.Player performance in close matches": """
                                              SELECT p.full_name,
                                                     ROUND(AVG(prf.runs_scored), 2) AS avg_runs,
                                                     COUNT(*) AS close_matches_played,
                                                     SUM(CASE WHEN m.winning_team = t.team_name THEN 1 ELSE 0 END) AS matches_won_by_team
                                              FROM player_recent_form prf
                                                       JOIN players p ON prf.player_id = p.player_id
                                                       JOIN matches m ON prf.match_id = m.match_id
                                                       JOIN teams t ON m.team1_id = t.team_id OR m.team2_id = t.team_id
                                              WHERE (m.victory_type = 'Runs' AND m.victory_margin < 50)
                                                 OR (m.victory_type = 'Wickets' AND m.victory_margin < 5)
                                              GROUP BY p.player_id;
                                              """,

    "16.Yearly batting performance since 2020": """
                                                SELECT p.full_name,
                                                    YEAR(prf.match_date) AS year,
                                                    ROUND(AVG(prf.runs_scored), 2) AS avg_runs_per_match,
                                                    ROUND(AVG(prf.strike_rate), 2) AS avg_strike_rate,
                                                    COUNT(*) AS matches_played
                                                FROM player_recent_form prf
                                                    JOIN players p ON prf.player_id = p.player_id
                                                WHERE prf.match_date >= '2020-01-01'
                                                GROUP BY p.player_id, YEAR(prf.match_date)
                                                HAVING COUNT(*) >= 5;
                                                """,

    "17.Toss Advantage Analysis": """
                                  SELECT toss_decision,
                                         COUNT(*) AS total_matches,
                                         SUM(CASE WHEN toss_winner = winning_team THEN 1 ELSE 0 END) AS toss_win_and_match_win,
                                         ROUND(SUM(CASE WHEN toss_winner = winning_team THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS win_percentage
                                  FROM matches
                                  WHERE match_status = 'Completed'
                                  GROUP BY toss_decision;
                                  """,
    "18.Most Economical Bowlers (ODI & T20)": """
                                              SELECT player_id,
                                                     COUNT(DISTINCT match_id) AS matches_played,
                                                     SUM(overs_bowled) AS total_overs,
                                                     SUM(runs_conceded) AS total_runs,
                                                     SUM(wickets_taken) AS total_wickets,
                                                     ROUND(SUM(runs_conceded) / SUM(overs_bowled), 2) AS economy_rate
                                              FROM bowling_stats
                                              WHERE format IN ('ODI', 'T20I')
                                              GROUP BY player_id
                                              HAVING COUNT(DISTINCT match_id) >= 10
                                                 AND SUM(overs_bowled) / COUNT(DISTINCT match_id) >= 2
                                              ORDER BY economy_rate ASC;
                                              """,

    "19.Consistent Batsmen (Low Std Dev)": """
                                           SELECT player_id,
                                                  ROUND(AVG(runs_scored), 2) AS avg_runs,
                                                  ROUND(STDDEV(runs_scored), 2) AS run_std_dev
                                           FROM player_recent_form
                                           WHERE balls_faced >= 10 AND match_date >= '2022-01-01'
                                           GROUP BY player_id
                                           HAVING COUNT(*) >= 5
                                           ORDER BY run_std_dev ASC;
                                           """,

    "20.Format-wise Match Count & Batting Avg": """
                                                SELECT player_id,
                                                       COUNT(DISTINCT match_id) AS matches_played,
                                                       ROUND(AVG(runs_scored), 2) AS batting_avg
                                                FROM batting_stats
                                                GROUP BY player_id
                                                HAVING SUM(CASE WHEN format IN ('Test', 'ODI', 'T20I') THEN 1 ELSE 0 END) >= 20;
                                                """,

    "21.Performance Ranking System": """
                                     SELECT player_id, format,
                                            SUM(total_runs) * 0.01 + AVG(batting_average) * 0.5 + AVG(strike_rate) * 0.3 AS batting_points,
                                            SUM(total_wickets) * 2 + (50 - AVG(bowling_average)) * 0.5 + (6 - AVG(economy_rate)) * 2 AS bowling_points,
                                            SUM(catches + stumpings) * 1.5 AS fielding_points,
                                            ROUND(
                                                    SUM(total_runs) * 0.01 + AVG(batting_average) * 0.5 + AVG(strike_rate) * 0.3 +
                                                    SUM(total_wickets) * 2 + (50 - AVG(bowling_average)) * 0.5 + (6 - AVG(economy_rate)) * 2 +
                                                    SUM(catches + stumpings) * 1.5, 2
                                            ) AS total_score
                                     FROM player_format_summary
                                              JOIN fielding_stats USING (player_id)
                                     GROUP BY player_id, format
                                     ORDER BY total_score DESC;
                                     """,

    "22.Head to Head Match Prediction": """
                                        SELECT team1_id, team2_id,
                                               COUNT(*) AS total_matches,
                                               SUM(CASE WHEN winning_team = team1_id THEN 1 ELSE 0 END) AS team1_wins,
                                               SUM(CASE WHEN winning_team = team2_id THEN 1 ELSE 0 END) AS team2_wins,
                                               ROUND(AVG(CASE WHEN winning_team = team1_id THEN victory_margin ELSE NULL END), 2) AS team1_avg_margin,
                                               ROUND(AVG(CASE WHEN winning_team = team2_id THEN victory_margin ELSE NULL END), 2) AS team2_avg_margin
                                        FROM matches
                                        WHERE match_date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
                                        GROUP BY team1_id, team2_id
                                        HAVING COUNT(*) >= 5;
                                        """,

    "23.Recent Form & Momentum": """
                                 WITH recent_form AS (
                                     SELECT player_id, match_date, runs_scored, strike_rate,
                                            ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date DESC) AS rn
                                     FROM player_recent_form
                                 )
                                 SELECT player_id,
                                        ROUND(AVG(CASE WHEN rn <= 5 THEN runs_scored END), 2) AS avg_last_5,
                                        ROUND(AVG(runs_scored), 2) AS avg_last_10,
                                        ROUND(STDDEV(runs_scored), 2) AS consistency_score,
                                        SUM(CASE WHEN runs_scored >= 50 THEN 1 ELSE 0 END) AS fifties_count,
                                        CASE
                                            WHEN STDDEV(runs_scored) < 10 AND AVG(runs_scored) > 50 THEN 'Excellent Form'
                                            WHEN STDDEV(runs_scored) < 15 THEN 'Good Form'
                                            WHEN STDDEV(runs_scored) < 25 THEN 'Average Form'
                                            ELSE 'Poor Form'
                                            END AS form_category
                                 FROM recent_form
                                 WHERE rn <= 10
                                 GROUP BY player_id;
                                 """,

    "24.Best Batting Partnership": """
                                   WITH quality_stats AS (
                                       SELECT player1_id AS batsman1_id, player2_id AS batsman2_id,
                                              COUNT(*) AS partnership_count,
                                              ROUND(AVG(partnership_runs), 2) AS avg_runs,
                                              MAX(partnership_runs) AS highest,
                                              CASE WHEN AVG(partnership_runs) > 50 THEN 'Good' ELSE 'Average' END AS good_partnerships
                                       FROM batting_partnerships
                                       WHERE batting_position1 = 1 AND batting_position2 = 2
                                       GROUP BY player1_id, player2_id
                                   )
                                   SELECT *
                                   FROM quality_stats
                                   ORDER BY avg_runs DESC
                                       LIMIT 10;
                                   """,

    "25.Timeseries Performance Evaluation": """SELECT  
                                              curr.player_id,
                                              CONCAT(curr.year, '-Q', curr.quarter) AS quarter_label,
                                              curr.avg_runs,
                                              curr.avg_sr,
                                              prev.avg_sr AS prev_avg_sr,
                                              CASE 
                                                WHEN curr.avg_sr > prev.avg_sr THEN 'Improving'
                                                WHEN curr.avg_sr < prev.avg_sr THEN 'Declining'
                                                ELSE 'Stable'
                                              END AS trend
                                            FROM (
                                              SELECT  
                                                player_id,
                                                YEAR(match_date) AS year,
                                                QUARTER(match_date) AS quarter,
                                                AVG(runs_scored) AS avg_runs,
                                                AVG(strike_rate) AS avg_sr,
                                                (YEAR(match_date) * 4 + QUARTER(match_date)) AS quarter_index
                                              FROM player_match
                                              GROUP BY player_id, YEAR(match_date), QUARTER(match_date), (YEAR(match_date) * 4 + QUARTER(match_date))
                                              HAVING COUNT(*) >= 3
                                            ) curr
                                            LEFT JOIN (
                                              SELECT  
                                                player_id,
                                                (YEAR(match_date) * 4 + QUARTER(match_date)) AS quarter_index,
                                                AVG(strike_rate) AS avg_sr
                                              FROM player_match
                                              GROUP BY player_id, (YEAR(match_date) * 4 + QUARTER(match_date))
                                              HAVING COUNT(*) >= 3
                                            ) prev
                                            ON curr.player_id = prev.player_id
                                            AND curr.quarter_index = prev.quarter_index + 1
                                            ORDER BY curr.player_id, curr.quarter_index;     
                                            """

        }
if selected_section == "📈SQL Analysis":
    st.header("📋 View SQL Analysis")

    # Dropdown for selecting query
    SQL_Analysis = st.selectbox("📈Select SQL Query", list(queries.keys()))

    # Connect to your database
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Akshiya13",
            database="cricbuzz"
        )
        cursor = conn.cursor()
        cursor.execute(queries[SQL_Analysis])
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)

        st.subheader(f"📊 Results: {SQL_Analysis}")
        st.dataframe(df)

    except Exception as e:
        st.error(f"❌ Error executing query: {e}")

    finally:
        if 'conn' in locals():
            conn.close()


# Footer
st.markdown("<div class='footer'>© Guvi 2025 CricBuzz Dashboard</div>", unsafe_allow_html=True)