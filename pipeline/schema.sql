-- This file should contain all code required to create & seed database tables.
DROP DATABASE IF EXISTS museum;

CREATE DATABASE museum;
\c museum;

-- Create tables
CREATE TABLE floor(
    floor_id INT GENERATED ALWAYS AS IDENTITY,
    floor_name VARCHAR(10) NOT NULL UNIQUE CHECK (TRIM(floor_name) != ''),
    PRIMARY KEY (floor_id)
);

CREATE TABLE department(
    department_id INT GENERATED ALWAYS AS IDENTITY,
    department_name VARCHAR(30) NOT NULL UNIQUE CHECK (TRIM(department_name) != ''),
    PRIMARY KEY (department_id)
);

CREATE TABLE request(
    request_id INT GENERATED ALWAYS AS IDENTITY,
    request_value INT NOT NULL UNIQUE,
    description VARCHAR(15) NOT NULL UNIQUE CHECK (TRIM(description) != ''),
    PRIMARY KEY (request_id)
);

CREATE TABLE rating(
    rating_id INT GENERATED ALWAYS AS IDENTITY,
    rating_value INT NOT NULL UNIQUE,
    description VARCHAR(15) NOT NULL UNIQUE CHECK (TRIM(description) != ''),
    PRIMARY KEY (rating_id)
);

CREATE TABLE exhibition(
    exhibition_id INT NOT NULL,
    exhibition_name VARCHAR(50) NOT NULL CHECK (TRIM(exhibition_name) != ''),
    floor_id INT NOT NULL,
    department_id INT,
    exhibition_start_date DATE NOT NULL CHECK (exhibition_start_date <= NOW()),
    description VARCHAR CHECK (TRIM(description) != ''),
    public_id varchar(10) NOT NULL UNIQUE CHECK (TRIM(public_id) != ''),
    PRIMARY KEY (exhibition_id),
    FOREIGN KEY (floor_id) REFERENCES floor(floor_id),
    FOREIGN KEY (department_id) REFERENCES department(department_id)
);

CREATE TABLE exhibition_request(
    exhibition_request_id INT GENERATED ALWAYS AS IDENTITY,
    time_at TIMESTAMPTZ CHECK (time_at <= NOW() + INTERVAL '5 minutes'),
    exhibition_id INT NOT NULL,
    request_id INT NOT NULL,
    UNIQUE (time_at, exhibition_id, request_id),
    PRIMARY KEY (exhibition_request_id),
    FOREIGN KEY (exhibition_id) REFERENCES exhibition(exhibition_id),
    FOREIGN KEY (request_id) REFERENCES request(request_id)
);

CREATE TABLE exhibition_rating(
    exhibition_rating_id INT GENERATED ALWAYS AS IDENTITY,
    time_at TIMESTAMPTZ CHECK (time_at <= NOW() + INTERVAL '5 minutes'),
    exhibition_id INT NOT NULL,
    rating_id INT NOT NULL,
    UNIQUE (time_at, exhibition_id, rating_id),
    PRIMARY KEY (exhibition_rating_id),
    FOREIGN KEY (exhibition_id) REFERENCES exhibition(exhibition_id),
    FOREIGN KEY (rating_id) REFERENCES rating(rating_id)
);


INSERT INTO floor (floor_name) VALUES ('Vault'), ('1'), ('2'), ('3');

INSERT INTO department (department_name)
    VALUES ('Zoology'), ('Entomology'), ('Geology'), ('Paleontology'), ('Ecology');

INSERT INTO request (request_value,description)
    VALUES (1,'Emergency'), (0,'Assistance');

INSERT INTO rating (rating_value, description)
    VALUES (0, 'Terrible'), (1, 'Bad'), (2, 'Neutral'), (3, 'Good'), (4, 'Amazing');

INSERT INTO exhibition (exhibition_id, exhibition_name, floor_id, department_id, exhibition_start_date, description, public_id)
    VALUES
        (
            1, 'Adaptation',
            (SELECT floor_id FROM floor WHERE floor_name = 'Vault'),
            (SELECT department_id FROM department WHERE department_name = 'Entomology'),
            '2019-07-01', 'How insect evolution has kept pace with an industrialised world', 'EXH_01'
        ),
        (
            0, 'Measureless to Man',
            (SELECT floor_id FROM floor WHERE floor_name = '1'),
            (SELECT department_id FROM department WHERE department_name = 'Geology'),
            '2021-08-23', 'An immersive 3D experience: delve deep into a previously-inaccessible cave system.', 'EXH_00'
        ),
        (
            5, 'Thunder Lizards',
            (SELECT floor_id FROM floor WHERE floor_name = '1'),
            (SELECT department_id FROM department WHERE department_name = 'Paleontology'),
            '2023-02-01', 'How new research is making scientists rethink what dinosaurs really looked like.', 'EXH_05'
        ),
        (
            2, 'The Crenshaw Collection',
            (SELECT floor_id FROM floor WHERE floor_name = '2'),
            (SELECT department_id FROM department WHERE department_name = 'Zoology'),
            '2021-03-03', 'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.', 'EXH_02'
        ),
        (
            4, 'Our Polluted World',
            (SELECT floor_id FROM floor WHERE floor_name = '3'),
            (SELECT department_id FROM department WHERE department_name = 'Ecology'),
            '2021-05-12', 'A hard-hitting exploration of humanity''s impact on the environment.', 'EXH_04'
        ),
        (
            3, 'Cetacean Sensations',
            (SELECT floor_id FROM floor WHERE floor_name = '1'),
            (SELECT department_id FROM department WHERE department_name = 'Zoology'),
            '2019-07-01', 'Whales: from ancient myth to critically endangered.', 'EXH_03'
        );

