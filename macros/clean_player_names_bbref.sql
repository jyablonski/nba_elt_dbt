/*
Standardizes player names by:
1. mapping specific players to standardized names because of historical bbref fuckups
2. removing common suffixes such as "Jr.", "Sr.", "II", "III", etc.
Usage: {{ clean_player_names_bbref('player') }}::text

Regex Explanation:
'\s+(Jr\.?|Sr\.?|II|III|IV|V)$'
     \s+     → One or more whitespace characters before the suffix
     ( ... ) → Match any one of the suffix options:
     Jr\.? → "Jr" optionally followed by a period
     Sr\.? → "Sr" optionally followed by a period
     II    → Roman numeral II
     III   → Roman numeral III
     IV    → Roman numeral IV
     V     → Roman numeral V
     $       → Match must be at the end of the string
'i'       → Case-insensitive match
*/
{% macro clean_player_names_bbref(column_name) %}
    -- the extension is created in the silver schema
    silver.unaccent(
        regexp_replace(
            CASE
                WHEN {{ column_name }} = 'Mo Bamba' THEN 'Mohamed Bamba'
                WHEN {{ column_name }} = 'Herb Jones' THEN 'Herbert Jones'
                WHEN {{ column_name }} = 'Nicolas Claxton' THEN 'Nic Claxton'
                WHEN {{ column_name }} = 'Enes Kanter' THEN 'Enes Freedom'
                WHEN {{ column_name }} = 'Cameron Thomas' THEN 'Cam Thomas'
                WHEN {{ column_name }} = 'Juan Hernangomez' THEN 'Juancho Hernangomez'
                WHEN {{ column_name }} = 'Didi Louzada' THEN 'Marcos Louzada Silva'
                WHEN {{ column_name }} = 'Wesley Iwundu' THEN 'Wes Iwundu'
                WHEN {{ column_name }} = 'Scotty Pippen ' THEN 'Scotty Pippen'
                ELSE {{ column_name }}
            END,
            '\s+(Jr\.?|Sr\.?|II|III|IV|V)$',
            '',
            'i'
        )
    )
{% endmacro %}