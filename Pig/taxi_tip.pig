a = load 'dataset/nyc_taxi_data_2014.csv' using PigStorage(',');
b = foreach a generate $3 as passengers, $15 as tip_amount;
passenger_group = group b by passengers;
c = foreach passenger_group generate group, COUNT(b), AVG(b.tip_amount);
d = order c by $2;
dump d;
