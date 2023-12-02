# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC   prepared_nba_data (
# MAGIC     Team_Rank int,
# MAGIC     Team_Name string,
# MAGIC     Average_Points double,
# MAGIC     Max_Points double,
# MAGIC     avg_field_goal_perc double,
# MAGIC     avg_defensive_rebound double,
# MAGIC     avg_offensive_rebound double,
# MAGIC     avg_turn_over double,
# MAGIC     avg_player_ranking double,
# MAGIC     min_player_ranking double,
# MAGIC     custom_metric double
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO
# MAGIC   prepared_nba_data
# MAGIC
# MAGIC -- Define a CTE to compute average statistics for each team
# MAGIC WITH team_stats AS (
# MAGIC     SELECT
# MAGIC         Tm,
# MAGIC         AVG(PTS) AS avg_pts,
# MAGIC         AVG(FGPerc) AS avg_fg_perc,
# MAGIC         AVG(DRB) AS avg_drb,
# MAGIC         AVG(ORB) AS avg_orb,
# MAGIC         AVG(TOV) AS avg_tov,
# MAGIC         AVG(Rk) AS avg_rk
# MAGIC     FROM nba_players_data_23
# MAGIC     GROUP BY Tm
# MAGIC ),
# MAGIC -- Define a CTE to compute the min-max values for each statistic of each team
# MAGIC -- where these min-max values are used to normalize the values of the statistics
# MAGIC -- in the next CTE
# MAGIC
# MAGIC min_max_values AS (
# MAGIC     SELECT
# MAGIC         Tm,
# MAGIC         MIN(PTS) AS min_pts,
# MAGIC         MAX(PTS) AS max_pts,
# MAGIC         MIN(FGPerc) AS min_fg_perc,
# MAGIC         MAX(FGPerc) AS max_fg_perc,
# MAGIC         MIN(DRB) AS min_drb,
# MAGIC         MAX(DRB) AS max_drb,
# MAGIC         MIN(ORB) AS min_orb,
# MAGIC         MAX(ORB) AS max_orb,
# MAGIC         MIN(TOV) AS min_tov,
# MAGIC         MAX(TOV) AS max_tov,
# MAGIC         MIN(RK) AS min_rk,
# MAGIC         MAX(RK) AS max_rk
# MAGIC     FROM nba_players_data_23
# MAGIC     GROUP BY Tm
# MAGIC ),
# MAGIC -- Define a CTE to compute the normalized values of each statistic of each team
# MAGIC -- based on the min-max normalization technique
# MAGIC
# MAGIC ranked_teams AS (
# MAGIC     SELECT
# MAGIC         Tm,
# MAGIC         avg_pts,
# MAGIC         avg_fg_perc,
# MAGIC         avg_drb,
# MAGIC         avg_orb,
# MAGIC         avg_tov,
# MAGIC         avg_rk,
# MAGIC         (
# MAGIC         avg_pts - (
# MAGIC             SELECT MIN(min_pts)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) / (
# MAGIC         (
# MAGIC             SELECT MAX(max_pts)
# MAGIC             FROM min_max_values
# MAGIC         ) - (
# MAGIC             SELECT MIN(min_pts)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) AS norm_pts,
# MAGIC         (
# MAGIC         avg_fg_perc - (
# MAGIC             SELECT MIN(min_fg_perc)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) / (
# MAGIC         (
# MAGIC             SELECT MAX(max_fg_perc)
# MAGIC             FROM min_max_values
# MAGIC         ) - (
# MAGIC             SELECT MIN(min_fg_perc)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) AS norm_fg_perc,
# MAGIC         (
# MAGIC         avg_drb - (
# MAGIC             SELECT MIN(min_drb)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) / (
# MAGIC         (
# MAGIC             SELECT MAX(max_drb)
# MAGIC             FROM min_max_values
# MAGIC         ) - (
# MAGIC             SELECT MIN(min_drb)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) AS norm_drb,
# MAGIC         (
# MAGIC         avg_orb - (
# MAGIC             SELECT MIN(min_orb)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) / (
# MAGIC         (
# MAGIC             SELECT MAX(max_orb)
# MAGIC             FROM min_max_values
# MAGIC         ) - (
# MAGIC             SELECT MIN(min_orb)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) AS norm_orb,
# MAGIC         (
# MAGIC         avg_tov - (
# MAGIC             SELECT MIN(min_tov)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) / (
# MAGIC         (
# MAGIC             SELECT MAX(max_tov)
# MAGIC             FROM min_max_values
# MAGIC         ) - (
# MAGIC             SELECT MIN(min_tov)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) AS norm_tov,
# MAGIC         (
# MAGIC         avg_rk - (
# MAGIC             SELECT MIN(min_rk)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) / (
# MAGIC         (
# MAGIC             SELECT MAX(max_rk)
# MAGIC             FROM min_max_values
# MAGIC         ) - (
# MAGIC             SELECT MIN(min_rk)
# MAGIC             FROM min_max_values
# MAGIC         )
# MAGIC         ) AS norm_rk,
# MAGIC         RANK() OVER (ORDER BY avg_pts DESC) AS team_rank
# MAGIC     FROM team_stats
# MAGIC ),
# MAGIC -- Define a CTE to rank the teams based on a custom metric computed for each team
# MAGIC
# MAGIC ranked_teams_2 AS (
# MAGIC     SELECT
# MAGIC         Tm,
# MAGIC         avg_pts,
# MAGIC         avg_fg_perc,
# MAGIC         avg_drb,
# MAGIC         avg_orb,
# MAGIC         avg_tov,
# MAGIC         norm_pts,
# MAGIC         avg_rk,
# MAGIC         norm_fg_perc,
# MAGIC         norm_drb,
# MAGIC         norm_orb,
# MAGIC         norm_tov,
# MAGIC         norm_rk,
# MAGIC         RANK() OVER (ORDER BY norm_pts - norm_tov + norm_drb DESC) AS team_rank
# MAGIC     FROM ranked_teams
# MAGIC )
# MAGIC -- Output the results by joining the ranked_teams_2 CTE with the min_max_values CTE
# MAGIC -- and filtering the top 10 teams
# MAGIC
# MAGIC SELECT
# MAGIC     team_rank,
# MAGIC     ranked_teams_2.Tm,
# MAGIC     ROUND(avg_pts, 2),
# MAGIC     -- the variable max_points is computed by joining with the min_max_values table
# MAGIC     -- and extracting the max_pts column
# MAGIC     ROUND(min_max_values.max_pts, 2) max_points,
# MAGIC     ROUND(avg_fg_perc, 2),
# MAGIC     ROUND(avg_drb, 2) avg_Dreb,
# MAGIC     ROUND(avg_orb, 2) avg_Oreb,
# MAGIC     ROUND(avg_tov, 2),
# MAGIC     ROUND(avg_rk, 2) avg_rannking,
# MAGIC     -- the variable min_ranking is computed by joining with the min_max_values table
# MAGIC     -- and extracting the min_rk column
# MAGIC     ROUND(min_max_values.min_rk, 2) min_ranking,
# MAGIC     ROUND(norm_pts - norm_tov + norm_drb, 2) as custom_metric
# MAGIC FROM
# MAGIC     ranked_teams_2
# MAGIC     LEFT JOIN min_max_values ON min_max_values.Tm = ranked_teams_2.Tm
# MAGIC WHERE
# MAGIC     team_rank <= 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prepared_nba_data

# COMMAND ----------

# Data quality validation
# Read the prepared_nba_data table into a Spark DataFrame
prepared_nba_data = spark.table("prepared_nba_data")

# Count the number of duplicate rows
duplicates = (
  prepared_nba_data
  .groupBy("Team_Rank", "Team_Name", "Average_Points", "Max_Points", "avg_field_goal_perc",
           "avg_defensive_rebound", "avg_offensive_rebound", "avg_turn_over",
           "avg_player_ranking", "min_player_ranking", "custom_metric")
  .count()
  .filter("count > 1")
)

# Assert that the count of duplicate rows is zero
assert duplicates.count() == 0, "Duplicate rows found in prepared_nba_data"
