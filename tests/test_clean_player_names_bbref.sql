with raw_names as (
    -- Manual overrides
    select
        'Mo Bamba' as input,
        'Mohamed Bamba' as expected
    union all
    select
        'Herb Jones',
        'Herbert Jones'
    union all
    select
        'Nicolas Claxton',
        'Nic Claxton'
    union all
    select
        'Enes Kanter',
        'Enes Freedom'
    union all
    select
        'Cameron Thomas',
        'Cam Thomas'
    union all
    select
        'Juan Hernangomez',
        'Juancho Hernangomez'
    union all
    select
        'Didi Louzada',
        'Marcos Louzada Silva'
    union all
    select
        'Wesley Iwundu',
        'Wes Iwundu'
    union all
    select
        'Scotty Pippen ',
        'Scotty Pippen'

    -- Suffix removals
    union all
    select
        'LeBron James Jr.',
        'LeBron James'
    union all
    select
        'Chris Paul II',
        'Chris Paul'
    union all
    select
        'Tim Duncan Sr.',
        'Tim Duncan'
    union all
    select
        'Gary Payton III',
        'Gary Payton'
    union all
    select
        'Scottie Barnes IV',
        'Scottie Barnes'
    union all
    select
        'Kevin Love Jr.',
        'Kevin Love'
    union all
    select
        'Marcus Smart II',
        'Marcus Smart'
    union all
    select
        'Jason Kidd Sr.',
        'Jason Kidd'

    -- No change expected
    union all
    select
        'Stephen Curry',
        'Stephen Curry'
    union all
    select
        'Jayson Tatum',
        'Jayson Tatum'
)

select *
from raw_names
where expected != ({{ clean_player_names_bbref('input') }}::text)
