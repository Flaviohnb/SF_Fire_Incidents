CREATE TABLE IF NOT EXISTS dim_calendar (
    date_full DATE,
    year INT,
    month INT,
    month_name VARCHAR(15),
    day INT,
    day_of_week_number INT,
    day_of_week_name VARCHAR(15),
    day_of_year INT,
    quarter_of_year VARCHAR(2),
    week_of_year INT
);