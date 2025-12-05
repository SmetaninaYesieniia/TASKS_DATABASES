DROP TABLE IF EXISTS raw_games;

CREATE TABLE raw_games AS
SELECT * FROM read_json_auto('/Users/yesieniia_s/Downloads/steam_2025_5k-dataset-games_20250831.json', maximum_object_size=1073741824);

DROP TABLE IF EXISTS raw_reviews;

CREATE TABLE raw_reviews AS
SELECT * FROM read_json_auto('/Users/yesieniia_s/Downloads/steam_2025_5k-dataset-reviews_20250901.json', maximum_object_size=1073741824);

DROP TABLE IF EXISTS games_flat;

CREATE TABLE games_flat AS
WITH exploded AS (
    SELECT UNNEST(games) AS game
    FROM raw_games
)
SELECT
    game.appid,
    game.app_details.data.name AS name,
    game.app_details.data.release_date.date AS release_date,
    game.app_details.data.is_free AS is_free,
    game.app_details.data.platforms.windows AS windows,
    game.app_details.data.platforms.mac AS mac,
    game.app_details.data.platforms.linux AS linux,
    game.app_details.data.genres AS genres
FROM exploded;

DROP TABLE IF EXISTS reviews_flat;

CREATE TABLE reviews_flat AS
WITH exploded AS (
    SELECT UNNEST(reviews) AS review
    FROM raw_reviews
)
SELECT
    review.appid,
    review.review_data.query_summary.num_reviews,
    review.review_data.query_summary.review_score,
    review.review_data.query_summary.review_score_desc,
    review.review_data.query_summary.total_positive,
    review.review_data.query_summary.total_negative,
    review.review_data.query_summary.total_reviews
FROM exploded;

--топ 20 ігор за відгуками
SELECT g.name, r.num_reviews
FROM games_flat g
JOIN reviews_flat r USING (appid)
ORDER BY r.num_reviews DESC
LIMIT 20;

--середні відгуки за жанрами
WITH genres_exploded AS (SELECT g.appid, g.name, r.total_reviews, UNNEST(g.genres).description AS genre
    FROM games_flat g
    JOIN reviews_flat r USING (appid)
)
SELECT
    genre,
    AVG(total_reviews) AS avg_total_reviews
FROM genres_exploded
GROUP BY genre
ORDER BY avg_total_reviews DESC;

--середні відгуки за платформами
SELECT
    CASE
        WHEN windows THEN 'Windows'
        WHEN mac THEN 'Mac'
        WHEN linux THEN 'Linux'
        ELSE 'Other'
    END AS platform,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM games_flat g
JOIN reviews_flat r USING (appid)
GROUP BY platform
ORDER BY avg_review_score DESC;

--розподіл за бешкоштовними чи платними іграми
SELECT
    CASE WHEN is_free THEN 'Free' ELSE 'Paid' END AS game_type,
    COUNT(*) AS num_games
FROM games_flat
GROUP BY game_type;

--топ 10 ігр за позитивними ревью
SELECT
    g.name,
    r.total_positive
FROM games_flat g
JOIN reviews_flat r USING (appid)
ORDER BY r.total_positive DESC
LIMIT 10;
