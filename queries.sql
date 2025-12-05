-- queries.sql

-- 1. Which movie has the highest average rating?
-- Robust top movie (min 50 ratings)
SELECT m.title, ROUND(AVG(r.rating),3) AS avg_rating, COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_id = m.id
GROUP BY m.id, m.title
HAVING COUNT(r.rating) >= 50
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;


-- 2. Top 5 movie genres that have the highest average rating
SELECT g.name AS genre, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(r.rating) AS num_ratings
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.id
JOIN ratings r ON r.movie_id = mg.movie_id
GROUP BY g.id, g.name
HAVING COUNT(r.rating) >= 10
ORDER BY avg_rating DESC
LIMIT 5;

-- 3. Director with the most movies in the dataset
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating of movies released each year
SELECT COALESCE(m.year, strftime('%Y', m.released)) AS year, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_id = m.id
GROUP BY year
ORDER BY year;
