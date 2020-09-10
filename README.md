# Pokestars

***NOTE*** : Please check out this link if embedded Tableau visual does not work on your device: https://public.tableau.com/profile/isaac.campbell.smith#!/vizhome/PokemonShowdown/Dashboard1

<p align="justify">
I've been a mega fan of competetive Pokemon battling since the release of Diamond and Pearl. The battle mechanics and wifi-features Nintendo introduced in 2007 vastly improved on what had already been a supremely addicting game, albeit a bit too dependent on luck and brute-force, and transformed it into an intellectually engaging strategy contest on par with chess. Pokemon Showdown has been central to my experience as it's open-source and free-to-play software allows battlers to simply create teams by defining all their own Pokemon's stats rather than having to spend hours mercilessly grinding in-game to make a perfect spread. They also provide a lot of the monthly battling data through <b>www.smogon.com</b>, which is perfect for interesting statistical analysis.  
<p align="justify">
My goal with this project is not only to build a richer understanding of the competitive landscape of Pokemon battling over the years and dive deeper into current trends to gain a competitive edge, but to enhance my exposure to an ETL data analysis workflow. After scraping all the data from Smogon, I set up a PostgreSQL Database on AWS and created 9 different tables to perform complex queries from. I've also started on some Tableau Dashboards to provide an overview of how things have changed over the years. 

<div class='tableauPlaceholder' id='viz1596748209067' style='position: center'><noscript><a href='#' width=1080><img alt=' ' src='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;Po&#47;PokemonShowdown&#47;Dashboard1&#47;1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='PokemonShowdown&#47;Dashboard1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;Po&#47;PokemonShowdown&#47;Dashboard1&#47;1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='en' /></object></div>

<br>

## Initial Findings
Even though more Pokemon have become available over time(Generation 7 in June 2016 and Generation 8 in January 2020), it seems like Showdown users are gravitating towards less variety.
<br><br>
<img src="https://raw.githubusercontent.com/isaac-campbell-smith/Pokestars/master/figures/TotalPokemonUsage.png">
<br><br>
<img src="https://raw.githubusercontent.com/isaac-campbell-smith/Pokestars/master/figures/Top3usage.png">
<br><br>
If you're interested in checking out the database, please shoot me a message at icampsmith@gmail.com 

I've also included all necessary scripts in the src to create your own version of this database along with a detailed walkthrough in the CreatingAnRDSonAWS.md. I highly recommend checking that out.

Here's how the database is structured: 

### pokemon
| id       |  name   | type_1       |  type_2   | hp       |  attack   | defense       |  sp_attack   | sp_defense       |  speed   |
| :------------- | :----------: | :------------- | :----------: | :------------- | :----------: | :------------- | :----------: | :------------- | :----------: |
| INT - <i> primary key | VARCHAR | VARCHAR | VARCHAR | INT | INT | INT | INT | INT | INT | 
| <i> unique id key | <i> i.e. Pikachu| <i> i.e. Electric| <i> i.e. None| | | | | | |
<br>

### battles

| id       |  count   | usage       |  month   | 
| :------------- | :----------: | :------------- | :----------: |
| INT | INT | REAL | VARCHAR | 
| <i> foreign pokemon key  | <i> total number of teams built with id| <i> pct monthly battles with id | |
<br>

### teammates
| id       |  mate_id   | x       |  month   | 
| :------------- | :----------: | :------------- | :----------: |
| INT | INT | REAL | VARCHAR | 
| <i> foreign pokemon key  | <i> foreign pokemon key | <i> numerator to calculate pct of teams having id with mate_id | | 
| | | <i> = x(mate_id) / usage(id) + usage(mate_id) |
<br>

### counters
| id         |  counter_id   | num_battles       |  check_pct   |      month   |
| :------------- | :----------: | :------------- | :----------: | :----------: |
| INT | INT | INT | REAL | VARCHAR | 
| <i> foreign pokemon key  | <i> foreign pokemon key | <i> matchups where id and mate_id faced off  |<i> percent of matchups where mate_id K.O.-d id or forced a switch | | 
<br>

### users
| month         |  num_battles   | 
| :------------- | :----------: |
| VARCHAR | INT - <i> total number of battles |
<br>

### natures
| id         |  name   | 
| :------------- | :----------: |
| INT - <i> primary key | VARCHAR - <i> i.e. Impish |
<br>

### battle_natures
| id         |  nature_id   | count       |      month   |
| :------------- | :----------: | :------------- | :----------: |
| INT | INT | INT | REAL | VARCHAR | 
| <i> foreign pokemon key  | <i> foreign natures key | <i> number of teams with id having nature_id  | |
<br>

### abilities
| id         |  name   | 
| :------------- | :----------: |
| INT - <i> primary key | VARCHAR - <i> i.e. Sheer Force |
<br>

### battle_abilities
| id         |  ability_id   | count       |      month   |
| :------------- | :----------: | :------------- | :----------: |
| INT | INT | INT | REAL | VARCHAR | 
| <i> foreign pokemon key  | <i> foreign abilities key | <i> number of teams with id having ability_id  | |
<br>

### items
| id         |  name   | 
| :------------- | :----------: |
| INT - <i> primary key | VARCHAR - <i> i.e. Sheer Force |
<br>

### battle_items
| id         |  item_id   | count       |      month   |
| :------------- | :----------: | :------------- | :----------: |
| INT | INT | INT | REAL | VARCHAR | 
| <i> foreign pokemon key  | <i> foreign items key | <i> number of teams with id having item_id  | |
<br>

