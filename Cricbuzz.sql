-- --- Cricbuzz DB creation -----
CREATE DATABASE cricbuzz;

-- --- Table creation & Insersion -----
use cricbuzz;
CREATE TABLE players (
    player_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    country VARCHAR(50),
    playing_role VARCHAR(30),  
    batting_style VARCHAR(30),
    bowling_style VARCHAR(50)
);

use cricbuzz;

select * from players;


CREATE TABLE teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(100),
    country VARCHAR(50)
);

select * from teams;


CREATE TABLE venues (
    venue_id INT PRIMARY KEY,
    venue_name VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    capacity INT
);

select * from venues;

CREATE TABLE series (
    series_id INT PRIMARY KEY,
    series_name VARCHAR(100),
    host_country VARCHAR(50),
    match_type VARCHAR(20),  -- Test, ODI, T20I
    start_date DATE,
    total_matches INT
);

select * from series;

CREATE TABLE matches (
    match_id INT PRIMARY KEY,
    match_description VARCHAR(200),
    match_date DATE,
    match_status VARCHAR(20),  -- Completed, Scheduled
    team1_id INT,
    team2_id INT,
    winning_team VARCHAR(100),
    victory_margin INT,
    victory_type VARCHAR(10),  -- Runs or Wickets
    venue_id INT,
    toss_winner VARCHAR(100),
    toss_decision VARCHAR(10),  -- Bat or Bowl
    FOREIGN KEY (team1_id) REFERENCES teams(team_id),
    FOREIGN KEY (team2_id) REFERENCES teams(team_id),
    FOREIGN KEY (venue_id) REFERENCES venues(venue_id)
);

select * from matches;


CREATE TABLE batting_stats (
    stat_id INT PRIMARY key AUTO_INCREMENT,
    player_id INT,
    match_id INT,
    format VARCHAR(10),  -- Test, ODI, T20I
    runs_scored INT,
    balls_faced INT,
    strike_rate float,
    batting_position INT,
    is_out BOOLEAN,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

select * from batting_stats;

CREATE TABLE bowling_stats (
    stat_id INT PRIMARY KEY,
    player_id INT,
    match_id INT,
    format VARCHAR(10),
    overs_bowled DECIMAL(4,1),
    runs_conceded INT,
    wickets_taken INT,
    economy_rate DECIMAL(4,2),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);


select * from bowling_stats;


CREATE TABLE player_match (
  player_id INT,
  match_date DATE,
  runs_scored INT,
  strike_rate DECIMAL(5,2)
);

select * from player_match;



CREATE TABLE fielding_stats (
    stat_id INT PRIMARY KEY,
    player_id INT,
    match_id INT,
    catches INT,
    stumpings INT,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

select * from fielding_stats;

CREATE TABLE batting_partnerships (
    partnership_id INT PRIMARY KEY,
    match_id INT,
    innings_number INT,
    player1_id INT,
    player2_id INT,
    batting_position1 INT,
    batting_position2 INT,
    partnership_runs INT,
    FOREIGN KEY (match_id) REFERENCES matches(match_id),
    FOREIGN KEY (player1_id) REFERENCES players(player_id),
    FOREIGN KEY (player2_id) REFERENCES players(player_id)
);


select * from batting_partnerships;

CREATE TABLE player_format_summary (
    player_id INT,
    format VARCHAR(10),
    total_runs INT,
    total_wickets INT,
    batting_average DECIMAL(5,2),
    bowling_average DECIMAL(5,2),
    strike_rate DECIMAL(5,2),
    economy_rate DECIMAL(5,2),
    centuries INT,
    matches_played INT,
    innings_played INT,
    PRIMARY KEY (player_id, format),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

select * from player_format_summary;

CREATE TABLE player_quarterly_stats (
    player_id INT,
    year INT,
    quarter INT,
    format VARCHAR(10),
    total_runs INT,
    strike_rate DECIMAL(5,2),
    matches_played INT,
    PRIMARY KEY (player_id, year, quarter, format),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

select * from player_quarterly_stats;

CREATE TABLE player_recent_form (
    player_id INT,
    match_id INT,
    runs_scored INT,
    balls_faced INT,
    strike_rate DECIMAL(5,2),
    is_above_50 BOOLEAN,
    match_date DATE,
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (match_id) REFERENCES matches(match_id)
);

select * from player_recent_form;




-- 1.Players who represent India
SELECT full_name, playing_role, batting_style, bowling_style
FROM players
WHERE country = 'India';


-- 2.Matches played in the last 30 days
SELECT 
    m.match_description,
    t1.team_name AS team1,
    t2.team_name AS team2,
    v.venue_name,
    v.city,
    m.match_date
FROM matches m
JOIN teams t1 ON m.team1_id = t1.team_id
JOIN teams t2 ON m.team2_id = t2.team_id
JOIN venues v ON m.venue_id = v.venue_id
WHERE m.match_date >= CURDATE() - INTERVAL 30 DAY
ORDER BY m.match_date DESC;

-- 3.Top 10 ODI run scorers
SELECT 
    p.full_name,
    pfs.total_runs,
    pfs.batting_average,
    pfs.centuries
FROM player_format_summary pfs
JOIN players p ON pfs.player_id = p.player_id
WHERE pfs.format = 'ODI'
ORDER BY pfs.total_runs DESC
LIMIT 10;

-- 4.Venues with capacity > 50,000
SELECT venue_name, city, country, capacity
FROM venues
WHERE capacity > 50000
ORDER BY capacity DESC;

-- 5.Matches won by each team
SELECT 
    t.team_name,
    COUNT(*) AS total_wins
FROM matches m
JOIN teams t ON m.winning_team = t.team_name
WHERE m.match_status = 'Completed'
GROUP BY t.team_name
ORDER BY total_wins DESC;

-- 6.Count of players by playing role
SELECT playing_role, COUNT(*) AS player_count
FROM players
GROUP BY playing_role;

-- 7.Highest individual score by format
SELECT format, MAX(runs_scored) AS highest_score
FROM batting_stats
GROUP BY format;

-- 8.Series started in 2024
SELECT series_name, host_country, match_type, start_date, total_matches
FROM series
WHERE YEAR(start_date) = 2024;

-- 9.All-rounders with 1000+ runs and 50+ wickets
SELECT 
    p.full_name,
    pfs.format,
    pfs.total_runs,
    pfs.total_wickets
FROM player_format_summary pfs
JOIN players p ON pfs.player_id = p.player_id
WHERE p.playing_role = 'All-rounder'
  AND pfs.total_runs > 1000
  AND pfs.total_wickets > 50;

-- 10.Last 20 completed matches
SELECT 
    m.match_description,
    t1.team_name AS team1,
    t2.team_name AS team2,
    m.winning_team,
    m.victory_margin,
    m.victory_type,
    v.venue_name,
    m.match_date
FROM matches m
JOIN teams t1 ON m.team1_id = t1.team_id
JOIN teams t2 ON m.team2_id = t2.team_id
JOIN venues v ON m.venue_id = v.venue_id
WHERE m.match_status = 'Completed'
ORDER BY m.match_date DESC
LIMIT 20;

-- 11.Player performance across formats
SELECT 
    p.full_name,
    MAX(CASE WHEN pfs.format = 'Test' THEN pfs.total_runs ELSE 0 END) AS test_runs,
    MAX(CASE WHEN pfs.format = 'ODI' THEN pfs.total_runs ELSE 0 END) AS odi_runs,
    MAX(CASE WHEN pfs.format = 'T20I' THEN pfs.total_runs ELSE 0 END) AS t20_runs,
    ROUND(AVG(pfs.batting_average), 2) AS overall_batting_avg
FROM player_format_summary pfs
JOIN players p ON pfs.player_id = p.player_id
GROUP BY p.player_id
HAVING COUNT(DISTINCT pfs.format) >= 2;

-- 12.Team wins at home vs away
SELECT 
    t.team_name,
    SUM(CASE WHEN t.country = v.country THEN 1 ELSE 0 END) AS home_wins,
    SUM(CASE WHEN t.country != v.country THEN 1 ELSE 0 END) AS away_wins
FROM matches m
JOIN teams t ON m.winning_team = t.team_name
JOIN venues v ON m.venue_id = v.venue_id
WHERE m.match_status = 'Completed'
GROUP BY t.team_name;

-- 13.Consecutive batsmen with 100+ partnership
SELECT 
    p1.full_name AS batsman1,
    p2.full_name AS batsman2,
    bp.partnership_runs,
    bp.innings_number
FROM batting_partnerships bp
JOIN players p1 ON bp.player1_id = p1.player_id
JOIN players p2 ON bp.player2_id = p2.player_id
WHERE ABS(bp.batting_position1 - bp.batting_position2) = 1
  AND bp.partnership_runs >= 100;


-- 14.Bowling performance at venues (≥3 matches, ≥4 overs)
SELECT 
    p.full_name,
    v.venue_name,
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

-- 15.Player performance in close matches
SELECT 
    p.full_name,
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

SELECT match_id, victory_type, victory_margin 
FROM matches 
WHERE (victory_type = 'Runs' AND victory_margin < 50)
   OR (victory_type = 'Wickets' AND victory_margin < 5);


select * FROM player_recent_form;


-- 16.Yearly batting performance since 2020
SELECT 
    p.full_name,
    YEAR(prf.match_date) AS year,
    ROUND(AVG(prf.runs_scored), 2) AS avg_runs_per_match,
    ROUND(AVG(prf.strike_rate), 2) AS avg_strike_rate,
    COUNT(*) AS matches_played
FROM player_recent_form prf
JOIN players p ON prf.player_id = p.player_id
WHERE prf.match_date >= '2020-01-01'
GROUP BY p.player_id, YEAR(prf.match_date)
HAVING COUNT(*) >= 5;

SELECT MIN(match_date), MAX(match_date) FROM player_recent_form;

SELECT COUNT(*) 
FROM player_recent_form prf
JOIN players p ON prf.player_id = p.player_id;

-- 17.Toss Advantage Analysis
SELECT 
  toss_decision,
  COUNT(*) AS total_matches,
  SUM(CASE WHEN toss_winner = winning_team THEN 1 ELSE 0 END) AS toss_win_and_match_win,
  ROUND(SUM(CASE WHEN toss_winner = winning_team THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS win_percentage
FROM matches
WHERE match_status = 'Completed'
GROUP BY toss_decision;

-- 18.Most Economical Bowlers (ODI & T20)
SELECT 
  player_id,
  COUNT(DISTINCT match_id) AS matches_played,
  SUM(overs_bowled) AS total_overs,
  SUM(runs_conceded) AS total_runs,
  SUM(wickets_taken) AS total_wickets,
  ROUND(SUM(runs_conceded) / SUM(overs_bowled), 2) AS economy_rate
FROM bowling_stats
WHERE format IN ('ODI', 'T20I')
GROUP BY player_id
HAVING COUNT(DISTINCT match_id) >= 10 AND SUM(overs_bowled) / COUNT(DISTINCT match_id) >= 2
ORDER BY economy_rate ASC;

-- 19. Consistent Batsmen (Low Std Dev)
SELECT 
  player_id,
  ROUND(AVG(runs_scored), 2) AS avg_runs,
  ROUND(STDDEV(runs_scored), 2) AS run_std_dev
FROM player_recent_form
WHERE balls_faced >= 10 AND match_date >= '2022-01-01'
GROUP BY player_id
HAVING COUNT(*) >= 5
ORDER BY run_std_dev ASC;

-- 20. Format-wise Match Count & Batting Avg
SELECT 
  player_id,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT match_id) AS matches_played,
  SUM(CASE WHEN format IN ('Test', 'ODI', 'T20I') THEN 1 ELSE 0 END) AS valid_format_rows,
  ROUND(AVG(runs_scored), 2) AS batting_avg
FROM batting_stats
GROUP BY player_id;


-- 21.Performance Ranking System
SELECT 
  player_id,
  format,
  -- Batting
  SUM(total_runs) * 0.01 + AVG(batting_average) * 0.5 + AVG(strike_rate) * 0.3 AS batting_points,
  -- Bowling
  SUM(total_wickets) * 2 + (50 - AVG(bowling_average)) * 0.5 + (6 - AVG(economy_rate)) * 2 AS bowling_points,
  -- Fielding
  SUM(catches + stumpings) * 1.5 AS fielding_points,
  -- Total
  ROUND(
    SUM(total_runs) * 0.01 + AVG(batting_average) * 0.5 + AVG(strike_rate) * 0.3 +
    SUM(total_wickets) * 2 + (50 - AVG(bowling_average)) * 0.5 + (6 - AVG(economy_rate)) * 2 +
    SUM(catches + stumpings) * 1.5, 2
  ) AS total_score
FROM player_format_summary
JOIN fielding_stats USING (player_id)
GROUP BY player_id, format
ORDER BY total_score DESC;

-- 22.Head to Head match prediction
SELECT
    LEAST(m.team1_id, m.team2_id) AS team_a,
    GREATEST(m.team1_id, m.team2_id) AS team_b,
    COUNT(*) AS total_matches,
    SUM(CASE WHEN m.winning_team = t1.team_name THEN 1 ELSE 0 END) AS team_a_wins,
    SUM(CASE WHEN m.winning_team = t2.team_name THEN 1 ELSE 0 END) AS team_b_wins,
    ROUND(AVG(CASE WHEN m.winning_team = t1.team_name THEN m.victory_margin ELSE NULL END), 2) AS team_a_avg_margin,
    ROUND(AVG(CASE WHEN m.winning_team = t2.team_name THEN m.victory_margin ELSE NULL END), 2) AS team_b_avg_margin
FROM matches m
JOIN teams t1 ON t1.team_id = LEAST(m.team1_id, m.team2_id)
JOIN teams t2 ON t2.team_id = GREATEST(m.team1_id, m.team2_id)
WHERE m.match_date >= DATE_SUB(CURDATE(), INTERVAL 3 YEAR)
  AND m.match_status = 'Completed'
GROUP BY team_a, team_b
HAVING COUNT(*) >= 5;


-- 23.Recent form & Momentum
WITH recent_form AS (
  SELECT 
    player_id,
    match_date,
    runs_scored,
    strike_rate,
    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY match_date DESC) AS rn
  FROM player_recent_form
)
SELECT 
  player_id,
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

-- 24. Best Batting Partnership
WITH quality_stats AS (
  SELECT 
    player1_id AS batsman1_id,
    player2_id AS batsman2_id,
    COUNT(*) AS partnership_count,
    ROUND(AVG(partnership_runs), 2) AS avg_runs,
    MAX(partnership_runs) AS highest,
    CASE 
      WHEN AVG(partnership_runs) > 50 THEN 'Good' 
      ELSE 'Average' 
    END AS good_partnerships
  FROM batting_partnerships
  WHERE batting_position1 = 1 AND batting_position2 = 2
  GROUP BY player1_id, player2_id
)
SELECT * 
FROM quality_stats
ORDER BY avg_runs DESC
LIMIT 10;


-- 25.Timeseries Perfomance evaluation
use cricbuzz;
SELECT  
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


