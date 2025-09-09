{% macro convert_team_names_flairs(column_name) %}

CASE 
     -- Atlanta Hawks
     WHEN {{ column_name }} IN ('Hawks', 'bwHawks', 'bwAtl', 'tHawks') THEN 'ATL'
     
     -- Boston Celtics  
     WHEN {{ column_name }} IN ('Celtics', 'bwBos', 'bwCeltics') THEN 'BOS'
     
     -- Brooklyn Nets
     WHEN {{ column_name }} IN ('Nets', 'bwNets', 'bwBkn', 'tNets') THEN 'BKN'
     
     -- Charlotte Hornets
     WHEN {{ column_name }} IN ('ChaHornets', 'Hornets', 'HornetsBandwagon', 'bwCha', 'OKCHornets') THEN 'CHA'
     
     -- Chicago Bulls
     WHEN {{ column_name }} IN ('Bulls', 'bwBulls', 'bwChi', 'tBulls') THEN 'CHI'
     
     -- Cleveland Cavaliers
     WHEN {{ column_name }} IN ('Cavaliers', 'CavaliersBandwagon', 'bwCavaliers', 'bwCavs') THEN 'CLE'
     
     -- Dallas Mavericks
     WHEN {{ column_name }} IN ('Mavs', 'bwDal', 'bwMavs', 'tMavs') THEN 'DAL'
     
     -- Denver Nuggets
     WHEN {{ column_name }} IN ('Nuggets', 'bwDen', 'bwNuggets') THEN 'DEN'
     
     -- Detroit Pistons
     WHEN {{ column_name }} IN ('Pistons', 'bwPistons') THEN 'DET'
     
     -- Golden State Warriors
     WHEN {{ column_name }} IN ('Warriors', 'WarriorsBandwagon', 'bwGsw', 'bwWarriors', 'bwWas') THEN 'GSW'
     
     -- Houston Rockets
     WHEN {{ column_name }} IN ('Rockets', 'SanDiegoRockets', 'bwRockets') THEN 'HOU'
     
     -- Indiana Pacers
     WHEN {{ column_name }} IN ('Pacers', 'bwPacers') THEN 'IND'
     
     -- LA Clippers
     WHEN {{ column_name }} IN ('Clippers', 'bwLac') THEN 'LAC'
     
     -- LA Lakers
     WHEN {{ column_name }} IN ('Lakers', 'bwLakers', 'bwLal', 'MinnLakers') THEN 'LAL'
     
     -- Memphis Grizzlies
     WHEN {{ column_name }} IN ('Grizzlies', 'VanGrizzlies', 'bwMem', 'tGrizzlies') THEN 'MEM'
     
     -- Miami Heat
     WHEN {{ column_name }} IN ('Heat', 'HeatBandwagon', 'bwMia', 'bwHeat') THEN 'MIA'
     
     -- Milwaukee Bucks
     WHEN {{ column_name }} IN ('Bucks', 'bwMil', 'bwBucks') THEN 'MIL'
     
     -- Minnesota Timberwolves
     WHEN {{ column_name }} IN ('Timberwolves', 'bwMin', 'bwTimberwolves', 'bwWolves') THEN 'MIN'
     
     -- New Orleans Pelicans
     WHEN {{ column_name }} IN ('Pelicans', 'bwNol') THEN 'NOP'
     
     -- New York Knicks
     WHEN {{ column_name }} IN ('Knicks', 'KnickerBockers', 'bwKnicks', 'bwNyk', 'tKnicks') THEN 'NYK'
     
     -- Oklahoma City Thunder
     WHEN {{ column_name }} IN ('Thunder', 'ThunderBandwagon', 'bwThunder') THEN 'OKC'
     
     -- Orlando Magic
     WHEN {{ column_name }} IN ('Magic', 'tMagic') THEN 'ORL'
     
     -- Portland Trail Blazers
     WHEN {{ column_name }} IN ('TrailBlazers', 'bwBlazers') THEN 'POR'
     
     -- Philadelphia 76ers
     WHEN {{ column_name }} IN ('76ers', '76ersBandwagon', 'bw76ers', 'bwPhi', 'bwSixers', 'PHI') THEN 'PHI'
     
     -- Phoenix Suns
     WHEN {{ column_name }} IN ('Suns', 'bwPhx', 'tSuns') THEN 'PHX'
     
     -- Sacramento Kings
     WHEN {{ column_name }} IN ('Kings', 'bwKings', 'bwSac', 'tKings') THEN 'SAC'
     
     -- San Antonio Spurs
     WHEN {{ column_name }} IN ('Spurs', 'SpursBandwagon', 'bwSas', 'bwSpurs') THEN 'SAS'
     
     -- Toronto Raptors
     WHEN {{ column_name }} IN ('Raptors', 'RaptorsBandwagon', 'TampaRaptors', 'bwRaptors', 'bwTor', 'TorHuskies') THEN 'TOR'
     
     -- Utah Jazz
     WHEN {{ column_name }} IN ('Jazz', 'bwJazz', 'bwUta') THEN 'UTA'
     
     -- Washington Wizards
     WHEN {{ column_name }} IN ('Wizards', 'Bullets', 'bwWas') THEN 'WAS'
     
     -- Historical/Defunct Teams
     WHEN {{ column_name }} IN ('SuperSonics', 'Supersonics') THEN 'SEA'
     WHEN {{ column_name }} IN ('Bobcats') THEN 'CHA'
     WHEN {{ column_name }} IN ('Braves') THEN 'LAC'
     
     -- Conference/League/General
     WHEN {{ column_name }} IN ('NBA', 'East', 'West', 'WEST') THEN 'NBA'
     
     -- Countries (for international players/fans)
     WHEN {{ column_name }} IN ('USA', 'CAN', 'Canada', 'AUS', 'Australia', 'China', 'CHN', 'France', 'FRA', 'Lithuania', 'LTU', 'Serbia', 'SRB', 'Slovenia', 'SLV', 'Spain', 'ESP', 'Germany', 'DEU', 'Finland', 'FIN', 'GreatBritain', 'NewZealand', 'NZL', 'Philippines', 'JPN', 'KOR', 'MEX', 'BRA', 'ARG', 'DOM', 'VEN', 'TUR', 'POL', 'PRT', 'ITA', 'GRE', 'EGY', 'NGR', 'CIV', 'GEO', 'IRN', 'JOR', 'LBN', 'LAT', 'CZE', 'CPV', 'GRD', 'SSD', 'TUN', 'MON') THEN 'INTERNATIONAL'
     
     -- Special cases
     WHEN {{ column_name }} IN ('LeBron James') THEN 'PLAYER'
     WHEN {{ column_name }} IN ('SGA') THEN 'PLAYER'
     WHEN {{ column_name }} IN ('bbref') THEN 'REFERENCE'
     WHEN {{ column_name }} IN ('Generals', 'GFL') THEN 'OTHER_LEAGUE'
     WHEN {{ column_name }} IN ('VOTE', 'YAC', 'SUP', 'ANG') THEN 'SPECIAL'
     WHEN {{ column_name }} IN ('none', '') THEN 'NO_FLAIR'
     
     -- Default case
     ELSE 'OTHER'
END

{% endmacro %}