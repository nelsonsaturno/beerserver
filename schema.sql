CREATE TABLE beer_record(
    id SERIAL PRIMARY KEY,
    "date" TIMESTAMP NOT NULL,
    beer_type VARCHAR(5) NOT NULL,
    total_ml FLOAT
);