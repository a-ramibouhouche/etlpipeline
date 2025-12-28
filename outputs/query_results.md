# Query Results

Database: `production.db`

## Total records loaded (sensor_readings)

```sql
SELECT COUNT(*) AS cnt FROM sensor_readings;
```

| cnt |
| --- |
| 10081 |



## Latest hourly summary for Line_1

```sql
SELECT * FROM hourly_summary
        WHERE line_id = 'Line_1'
        ORDER BY hour DESC
        LIMIT 10;
```

| summary_id | hour | line_id | machine_id | avg_temperature | min_temperature | max_temperature | avg_pressure | avg_vibration | total_checks | defect_count | defect_rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11823 | 2025-03-11 10:00:00 | Line_1 | machine_9 | 73.61500000000001 | 58.22 | 89.01 | 3.98 | 63.025 | 2 | 1 | 50.0 |
| 11822 | 2025-03-11 10:00:00 | Line_1 | machine_5 | 73.735 | 67.5 | 79.97 | 2.45 | 54.349999999999994 | 2 | 1 | 50.0 |
| 11821 | 2025-03-11 10:00:00 | Line_1 | machine_3 | 71.47 | 71.47 | 71.47 | 1.83 | 54.34 | 1 | 0 | 0.0 |
| 11820 | 2025-03-11 10:00:00 | Line_1 | machine_10 | 81.74666666666667 | 66.55 | 93.01 | 3.2433333333333336 | 60.31 | 3 | 0 | 0.0 |
| 11791 | 2025-03-11 09:00:00 | Line_1 | machine_8 | 71.14 | 71.14 | 71.14 | 4.48 | 80.03 | 1 | 0 | 0.0 |
| 11790 | 2025-03-11 09:00:00 | Line_1 | machine_4 | 71.88499999999999 | 67.61 | 76.16 | 2.21 | 29.22 | 2 | 0 | 0.0 |
| 11789 | 2025-03-11 09:00:00 | Line_1 | machine_3 | 89.36 | 89.36 | 89.36 | 3.12 | 50.22 | 1 | 0 | 0.0 |
| 11788 | 2025-03-11 09:00:00 | Line_1 | machine_2 | 79.99 | 79.99 | 79.99 | 3.06 | 31.57 | 1 | 0 | 0.0 |
| 11787 | 2025-03-11 09:00:00 | Line_1 | machine_1 | 68.81 | 68.81 | 68.81 | 4.05 | 34.89 | 1 | 1 | 100.0 |
| 11759 | 2025-03-11 08:00:00 | Line_1 | machine_9 | 73.17 | 65.08 | 82.23 | 1.99 | 59.35 | 3 | 0 | 0.0 |



## High defect rate hours (> 5%)

```sql
SELECT hour, line_id, machine_id, defect_rate
        FROM hourly_summary
        WHERE defect_rate > 5.0
        ORDER BY defect_rate DESC
        LIMIT 20;
```

| hour | line_id | machine_id | defect_rate |
| --- | --- | --- | --- |
| 2025-03-10 18:00:00 | Line_1 | machine_1 | 100.0 |
| 2025-03-10 18:00:00 | Line_1 | machine_7 | 100.0 |
| 2025-03-10 18:00:00 | Line_3 | machine_23 | 100.0 |
| 2025-03-10 18:00:00 | Line_5 | machine_46 | 100.0 |
| 2025-03-10 19:00:00 | Line_2 | machine_20 | 100.0 |
| 2025-03-10 19:00:00 | Line_4 | machine_31 | 100.0 |
| 2025-03-10 19:00:00 | Line_4 | machine_36 | 100.0 |
| 2025-03-10 19:00:00 | Line_5 | machine_42 | 100.0 |
| 2025-03-10 19:00:00 | Line_5 | machine_44 | 100.0 |
| 2025-03-10 19:00:00 | Line_5 | machine_45 | 100.0 |
| 2025-03-10 20:00:00 | Line_1 | machine_10 | 100.0 |
| 2025-03-10 20:00:00 | Line_1 | machine_5 | 100.0 |
| 2025-03-10 20:00:00 | Line_1 | machine_9 | 100.0 |
| 2025-03-10 20:00:00 | Line_2 | machine_11 | 100.0 |
| 2025-03-10 20:00:00 | Line_2 | machine_13 | 100.0 |
| 2025-03-10 20:00:00 | Line_3 | machine_30 | 100.0 |
| 2025-03-10 20:00:00 | Line_4 | machine_32 | 100.0 |
| 2025-03-10 20:00:00 | Line_4 | machine_38 | 100.0 |
| 2025-03-10 20:00:00 | Line_5 | machine_43 | 100.0 |
| 2025-03-10 21:00:00 | Line_1 | machine_4 | 100.0 |



## Data quality distribution

```sql
SELECT data_quality, COUNT(*) AS count
        FROM sensor_readings
        GROUP BY data_quality;
```

| data_quality | count |
| --- | --- |
| estimated | 8 |
| good | 10073 |



## Join sensor data with quality checks (sample)

```sql
SELECT
          s.timestamp,
          s.machine_id,
          s.temperature,
          q.result AS quality_result
        FROM sensor_readings s
        LEFT JOIN quality_checks q
          ON s.machine_id = q.machine_id
         AND s.timestamp = q.timestamp
        LIMIT 10;
```

| timestamp | machine_id | temperature | quality_result |
| --- | --- | --- | --- |
| 2025-03-04 11:30:00 | machine_1 | 62.79 |  |
| 2025-03-04 11:35:00 | machine_1 | 75.38 |  |
| 2025-03-04 12:11:00 | machine_1 | 65.37 |  |
| 2025-03-04 12:47:00 | machine_1 | 62.07 |  |
| 2025-03-04 13:02:00 | machine_1 | 73.12 |  |
| 2025-03-04 16:01:00 | machine_1 | 83.47 |  |
| 2025-03-04 16:33:00 | machine_1 | 72.97 |  |
| 2025-03-04 16:38:00 | machine_1 | 74.51 |  |
| 2025-03-04 18:09:00 | machine_1 | 72.24 |  |
| 2025-03-04 20:04:00 | machine_1 | 80.55 |  |



## Average temperature by machine

```sql
SELECT machine_id,
               AVG(temperature) AS avg_temp,
               MIN(temperature) AS min_temp,
               MAX(temperature) AS max_temp
        FROM sensor_readings
        GROUP BY machine_id
        ORDER BY machine_id;
```

| machine_id | avg_temp | min_temp | max_temp |
| --- | --- | --- | --- |
| machine_1 | 75.40983870967742 | 45.74 | 98.14 |
| machine_10 | 75.90897674418605 | 48.57 | 102.24 |
| machine_11 | 75.29402010050251 | 45.46 | 100.39 |
| machine_12 | 75.1414705882353 | 45.38 | 103.65 |
| machine_13 | 75.78517412935324 | 49.33 | 108.37 |
| machine_14 | 74.47165745856354 | 37.18 | 98.65 |
| machine_15 | 74.43446601941747 | 45.06 | 99.98 |
| machine_16 | 75.29473404255319 | 51.67 | 102.12 |
| machine_17 | 73.87367088607596 | 44.15 | 100.28 |
| machine_18 | 75.58789215686275 | 52.65 | 97.93 |
| machine_19 | 75.60852459016394 | 42.93 | 101.03 |
| machine_2 | 74.42774193548387 | 46.37 | 101.07 |
| machine_20 | 74.04370370370371 | 50.97 | 93.63 |
| machine_21 | 74.6041116751269 | 45.4 | 101.85 |
| machine_22 | 74.46378238341968 | 40.19 | 95.75 |
| machine_23 | 76.22053658536585 | 48.35 | 104.28 |
| machine_24 | 74.6071978021978 | 52.16 | 92.13 |
| machine_25 | 75.54611940298507 | 52.73 | 107.28 |
| machine_26 | 74.926 | 48.82 | 104.05 |
| machine_27 | 75.15318840579711 | 43.48 | 101.05 |
| machine_28 | 76.27387096774194 | 51.17 | 102.8 |
| machine_29 | 75.56176470588235 | 50.46 | 101.6 |
| machine_3 | 74.71328205128205 | 49.94 | 117.27 |
| machine_30 | 74.01668085106382 | 39.4 | 99.19 |
| machine_31 | 73.94303317535545 | 53.24 | 98.72 |
| machine_32 | 74.7824886877828 | 42.53 | 103.9 |
| machine_33 | 75.01018691588786 | 45.61 | 99.72 |
| machine_34 | 73.18187817258884 | 45.1 | 97.54 |
| machine_35 | 73.84847826086957 | 49.38 | 100.29 |
| machine_36 | 75.58375 | 41.1 | 103.22 |
| machine_37 | 74.70375 | 48.21 | 109.32 |
| machine_38 | 75.12834146341463 | 45.64 | 102.24 |
| machine_39 | 74.24835164835166 | 47.06 | 103.84 |
| machine_4 | 74.6441237113402 | 46.52 | 100.22 |
| machine_40 | 75.04004444444443 | 46.35 | 98.14 |
| machine_41 | 74.88672413793104 | 49.18 | 104.99 |
| machine_42 | 76.24887323943662 | 43.97 | 113.8 |
| machine_43 | 75.9733014354067 | 52.9 | 103.18 |
| machine_44 | 75.06474489795919 | 44.94 | 100.39 |
| machine_45 | 74.37392523364487 | 40.61 | 99.37 |
| machine_46 | 75.78076086956521 | 51.36 | 97.91 |
| machine_47 | 74.4941304347826 | 50.82 | 117.98 |
| machine_48 | 75.37724489795919 | 47.58 | 99.07 |
| machine_49 | 75.22281914893617 | 47.86 | 102.61 |
| machine_5 | 74.96727272727273 | 47.87 | 102.93 |
| machine_50 | 75.89035175879397 | 49.69 | 103.87 |
| machine_6 | 75.63077319587629 | 41.35 | 101.24 |
| machine_7 | 74.52735294117647 | 47.74 | 108.42 |
| machine_8 | 75.54069306930693 | 43.32 | 110.66 |
| machine_9 | 75.53401960784313 | 48.29 | 101.18 |


