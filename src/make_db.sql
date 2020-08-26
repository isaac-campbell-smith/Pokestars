CREATE TABLE pokemon (id INTEGER, name VARCHAR, type_1 VARCHAR, type_2 VARCHAR);
\copy pokemon FROM '/home/ubuntu/pokemon_data/pokemon_withtypes_reference.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE abilities (id INTEGER, name VARCHAR);
\copy abilities FROM '/home/ubuntu/pokemon_data/ability_reference.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE natures (id INTEGER, name VARCHAR);
\copy natures FROM '/home/ubuntu/pokemon_data/nature_reference.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE items (id INTEGER, name VARCHAR);
\copy items FROM '/home/ubuntu/pokemon_data/item_reference.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE battles (id INTEGER, month VARCHAR, count INTEGER, usage REAL);
\copy battles FROM '/home/ubuntu/pokemon_data/battle_counts.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE battle_natures (id INTEGER, nature_id INTEGER, count REAL, month VARCHAR);
\copy battle_natures FROM '/home/ubuntu/pokemon_data/nature_counts.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE battle_abilities (id INTEGER, ability_id INTEGER, count REAL, month VARCHAR);
\copy battle_abilities FROM '/home/ubuntu/pokemon_data/ability_counts.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE battle_items (id INTEGER, item_id INTEGER, count REAL, month VARCHAR);
\copy battle_items FROM '/home/ubuntu/pokemon_data/item_counts.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE teammates (id INTEGER, mate_id INTEGER, x REAL, month VARCHAR);
\copy teammates FROM '/home/ubuntu/pokemon_data/teammate_stats.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE counters (id INTEGER, counter_id INTEGER, num_battles REAL, check_pct REAL, month VARCHAR);
\copy counters FROM '/home/ubuntu/pokemon_data/counter_stats.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE users (month VARCHAR, num_battles INTEGER);
\copy users FROM '/home/ubuntu/pokemon_data/monthly_popularity.csv' DELIMITER ',' CSV HEADER;
