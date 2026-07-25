select date_day::date as date_day
from generate_series(
        '2024-01-01'::date,
        current_date + interval '5 years',
        interval '1 day'
    ) as dates (date_day)
