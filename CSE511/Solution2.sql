-- question 1
CREATE TABLE IF NOT EXISTS query1 AS
SELECT genres.name, COUNT(hasagenre.genreid) AS moviecount
FROM genres, hasagenre
WHERE genres.genreid = hasagenre.genreid
GROUP BY genres.name;

-- question 2
CREATE TABLE IF NOT EXISTS query2 AS
SELECT genres.name, AVG(ratings.rating) AS rating
FROM genres, ratings
WHERE EXISTS (
	SELECT genres.genreid, hasagenre.movieid
	FROM hasagenre
	WHERE hasagenre.movieid = ratings.movieid
		AND genres.genreid = hasagenre.genreid
)
GROUP BY genres.name;

-- question 3
CREATE TABLE IF NOT EXISTS query3 AS
SELECT title, CountOfRatings
FROM (
	SELECT movies.title, COUNT(ratings.rating) AS CountOfRatings
	FROM movies, ratings
	WHERE movies.movieid = ratings.movieid
	GROUP BY movies.title
) as temp
WHERE CountOfRatings >= 10;

-- question 4
CREATE TABLE IF NOT EXISTS query4 AS
SELECT movies.movieid, movies.title
FROM movies
WHERE movies.movieid IN (
	SELECT hasagenre.movieid
	FROM hasagenre
	WHERE hasagenre.genreid IN (
		SELECT genres.genreid
		FROM genres
		WHERE genres.name = 'Comedy'
	)
)
GROUP BY movies.movieid;

-- question 5
CREATE TABLE IF NOT EXISTS query5 AS
SELECT movies.title, AVG(ratings.rating) AS average
FROM movies, ratings
WHERE movies.movieid = ratings.movieid
GROUP BY movies.title;

-- question 6
CREATE TABLE IF NOT EXISTS query6 AS
SELECT AVG(ratings.rating) AS average
FROM ratings
WHERE ratings.movieid IN (
	SELECT hasagenre.movieid
	FROM hasagenre
	WHERE hasagenre.genreid IN (
		SELECT genres.genreid
		FROM genres
		WHERE genres.name = 'Comedy'
	)
);

-- question 7
CREATE TABLE IF NOT EXISTS query7 AS
SELECT AVG(ratings.rating) AS average
FROM ratings
WHERE ratings.movieid IN (
	(
		SELECT hasagenre.movieid
		FROM hasagenre
		WHERE hasagenre.genreid IN (
			SELECT genres.genreid
			FROM genres
			WHERE genres.name='Comedy'
		)
	) INTERSECT (
		SELECT hasagenre.movieid
		FROM hasagenre
		WHERE hasagenre.genreid IN (
			SELECT genres.genreid
			FROM genres
			WHERE genres.name='Romance'
		)
	)
);

-- question 8
CREATE TABLE IF NOT EXISTS query8 AS
SELECT AVG(ratings.rating) AS average
FROM ratings
WHERE ratings.movieid IN (
	SELECT hasagenre.movieid
	FROM hasagenre
	WHERE hasagenre.genreid IN (
		SELECT genres.genreid
		FROM genres
		WHERE genres.name='Romance'
	) EXCEPT (
		SELECT hasagenre.movieid
		FROM hasagenre
		WHERE hasagenre.genreid IN (
			SELECT genres.genreid
			FROM genres
			WHERE genres.name='Comedy'
		)
	)
);

-- question 9
CREATE TABLE IF NOT EXISTS query9 AS
SELECT movieid, rating
from ratings
WHERE userid = :v1;