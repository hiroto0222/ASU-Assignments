CREATE TABLE users(
    userid INTEGER,
    name VARCHAR(30) NOT NULL,
    PRIMARY KEY(userid)    
);

CREATE TABLE movies(
    movieid INTEGER,
    title VARCHAR(255) NOT NULL,
    PRIMARY KEY(movieid)
);

CREATE TABLE taginfo(
    tagid INTEGER,
    content VARCHAR(255) NOT NULL,
    PRIMARY KEY(tagid)
);

CREATE TABLE genres(
    genreid INTEGER,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY(genreid)
);

CREATE TABLE ratings(
    userid INTEGER NOT NULL,
    movieid INTEGER NOT NULL,
    rating NUMERIC CHECK (rating >= 0 AND rating <= 5) NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY(userid, movieid),
    FOREIGN KEY(userid) REFERENCES users ON DELETE CASCADE,
    FOREIGN KEY(movieid) REFERENCES movies ON DELETE CASCADE
);

CREATE TABLE tags(
    userid INTEGER NOT NULL,
    movieid INTEGER NOT NULL,
    tagid INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    PRIMARY KEY(userid, movieid, tagid),
    FOREIGN KEY(userid) REFERENCES users ON DELETE CASCADE,
    FOREIGN KEY(movieid) REFERENCES movies ON DELETE CASCADE,
    FOREIGN KEY(tagid) REFERENCES taginfo ON DELETE CASCADE
);

CREATE TABLE hasagenre(
    movieid INTEGER NOT NULL,
    genreid INTEGER NOT NULL,
    PRIMARY KEY(movieid, genreid),
    FOREIGN KEY(movieid) REFERENCES movies ON DELETE CASCADE,
    FOREIGN KEY(genreid) REFERENCES genres ON DELETE CASCADE
);