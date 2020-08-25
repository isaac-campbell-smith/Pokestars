# Pokestars

***NOTE*** : Please check out this link if embedded Tableau visual does not work on your device: https://public.tableau.com/profile/isaac.campbell.smith#!/vizhome/PokemonShowdown/Dashboard1

<p align="justify">
I've been mega fan of competetive Pokemon battling since the release of Diamond and Pearl. The battle mechanics Nintendo introduced in 2007 vastly improved on what had already been a supremely addicting game, albeit a bit too dependent on luck and brute-force, and transformed it into an intellectually engaging strategy contest on par with chess. Pokemon Showdown has been central to my experience as it's open-source and free-to-play software allows battlers to simply create teams by defining all their own Pokemon's stats rather than having to spend hours mercilessly grinding in-game to make a perfect spread. They also provide a lot of the monthly battling data through <b>www.smogon.com</b>, which is perfect for interesting statistical analysis.  
<p align="justify">
My goal with this project is not only to build a richer understanding of the competitive landscape of Pokemon battling over the years, but to enhance my exposure to an ETL data analysis workflow. After scraping all the data from smogon, I set up an PostgreSQL Database on AWS and created 9 different tables to perform complex queries from. I've also started on some Tableau Dashboards to provide an overview of how things have changed over the years. 

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

Here's how it's set up: 
<br><br>
| pokemon      |     |
| :------------- | :----------: |
| id     | INT - <i> unique id key|
| name    | VARCHAR - <i> i.e. Pikachu|

| battles      |     |
| :------------- | :----------: |
| id     | INT - <i> unique id key |
| count     | INT - <i> total number of teams built with id |
| usage    | REAL - <i> percent of battles in which pokemon was actually sent out |
| month    | DATE |

| teammates     |     |
| :------------- | :----------: |
| id     | INT - <i> unique id key |
| mate_id    | INT - <i> unique id key (same as from pokemon primary table) |
| x     | REAL - <i> numerator to calculate pct of teams having id with mate_id  
| | <i> = x(mate_id) / usage(id) + usage(mate_id)
| month    | DATE |

| counters      |     |
| :------------- | :----------: |
| id     | INT - <i> unique id key |
| counter_id    | INT - <i> unique id key (same as from pokemon primary table) |
| num_battles    | INT - <i> matchups where id and mate_id faced off |
| check_pct    | REAL - <i> percent of matchups where mate_id K.O.-d id or forced a switch |
| month    | DATE |

| users      |     |
| :------------- | :----------: |
| month     | DATE |
| num_battles    | INT - <i> total number of battles |

| natures      |     |
| :------------- | :----------: |
| id     | INT - <i> unique nature id key |
| name   | VARCHAR - <i> i.e. Impish |

| battle_natures      |     |
| :------------- | :----------: |
| id     | INT - <i> unique pokemon id key |
| nature_id    | INT - <i> unique nature id key |
| count    | INT - <i> number of teams with id having nature_id |
| month    | DATE |

| abilities      |     |
| :------------- | :----------: |
| id     | INT - <i> unique ability id key |
| name   | VARCHAR - <i> i.e. Sheer Force |

| battle_abilities      |     |
| :------------- | :----------: |
| id     | INT - <i> unique pokemon id key |
| ability_id    | INT - <i> unique ability id key |
| count    | INT - <i> number of teams with id having ability_id |
| month    | DATE |
<br>

## How to Set Up an AWS Relational Database (RDS)

Since introducing this project to a number of colleagues, I was a bit surprised that the most interesting component to folks is actually getting this database up an running. The AWS platform can be a bit of a pain to work with, so I get it, and I'm here for you! These are the essential steps I took to set up the database.

### Step 1: Get Data

OK, so maybe this is an obtuse start to a step-by-step guide, but I feel it's important to note how I went about doing setting up a SQL database and that there are multiple ways to accomplish Step 1. I chose to clean, assign unique IDs across related DataFrames using the Python Pandas library. Check out the src folder in this repo for how I did it. The code I wrote is pretty distinct to the JSON files I gathered from smogon.com but will be modularized further as I do more of this in the future. The same effect can be accomplished more elegantly using only SQL but I didn't want to fuss with it here as I've built SQL databases before and it can be a pain to get right. 

TL;DR -- Store database 'tables' as separate csv files (one for each table in your database). 

### Step 2: Transfer Data to S3 Bucket

This should be the easieset step. Create a new S3 Bucket to store your csv's. Check out this link if you need further help:
https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-1-create-bucket.html

### Step 3: Create an RDS Instance

Start by navigating to the RDS section of your AWS dashboard and create a new RDS. The GUI guides you toward a lot of default settings that you don't need to play with too much. Just make sure you write down your database name, username and password for later. 

The important part here is how you configure the security group (you'll probably need to create a new one). You'd think that telling the inbound rules to accept all incoming traffic will do the trick; for me it didn't. Instead I had to explicitly use these settings: <br><br>
<img src='https://raw.githubusercontent.com/isaac-campbell-smith/Pokestars/master/figures/RDS%20Security%20Settings.png' width='500'><br>

The HTTP and SSH rules allow you to interact with the EC2 instance you'll set up in the next step. The PostgreSQL rule allows you to populate the database from Pgserv (I'm not sure why the PostgreSQL name generated 2 different rules when I did it - one should be fine). I used the port range 5432 because that's the default standard associated with Postgres. You'll need to tailor that rule to whatever SQL server you use as they're all different. Security outbound rules can be set to 'All Traffic'. 

Once you click create, AWS will generate an Endpoint for the RDS. That address is how you'll connect to the database from your SQL software, so copy it down for later. Finally, also make note of the security group name that is given to your RDS. You'll need that for the EC2 instance you'll set up next. 

After this step you should have saved 5 items: a database name, username, password, endpoint address, and RDS security group name. 

### Step 4: Create an EC2 Instance

This step is fairly similar to the above but through the EC2 section of AWS. When assigning your security make sure it's the same security group that you created above.

### Step 5: Populate RDS From EC2

Once you're in your EC2 terminal, run the following commands.<br>


<li> <code>sudo apt update
<li> sudo apt upgrade
<li> wget -S -T 10 -t 5 https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh -O $HOME/anaconda.sh
<li> bash anaconda.sh 
</code> (<i> answer yes to all prompts </i>)
<code>
<li> source ~/.bashrc
<li> pip install awscli 
<code>
<li> pip install psycopg2</code> (<i> not necessary with aws s3 sync method </i>)
</code><br><br>

Install and verify that postgres is enabled and accepting connections at port 5432
<code>
<li> sudo apt install postgresql
<li> sudo systemcl is-active postgresql
<li> sudo systemctl is-enabled postgresql
<li> sudo systemctl status postgresql
</code><br><br>

Next, access your S3 bucket. There are multiple ways to do this but what I did initially was:
<code>
<li> aws s3 sync s3://< BUCKETNAME >/ /home/ubuntu/
</code><br><br>

My files aren't too large so I didn't really care much about storing them locally on my ec2 instance, which is what the sync method does. I've since written a script which incorporates psycopg2 to stream data from the s3 bucket so you don't have to store it locally. It requires about twice as much code but is ultimately a much safer route cost-wise if you have super big tables (no different in terms of time as far as I can tell). If you don't have a whole lot of data you're working with, read on. Otherwise checkout the data_to_db.py file in the src folder (which is technically the last step if you go that route).

Finally, connect to the database with the appropriate modifications to this code:<br>
<code>
psql -U postgres \
   --host= < ENDPOINT ADDRESS > \
   --port=5432 \
   --username=< USERNAME > \
   --password=< PASSWORD > \
   --dbname= < DBNAME >
</code>
<br><br>
AND dump the csvs into tables as desired. Check out the make_db.sql file in the src folder for that syntax. I personally couldn't be bothered with actually importing that file into my instance and running it as a command. Once you're in the database from the psql shell you can just copy-paste everything in. Shout out to Matthew Layne for a very clear tutorial regarding this part:
https://dataschool.com/learn-sql/importing-data-from-csv-in-postgresql/

### Step 6: You Did It

And that's it! You'll want to verify that you can actually connect to and query the database from outside the instance (check out my SQL eda notebook in the notebooks folder if you need inspiration). Once you've verified that, you should shut down your EC2 instance and delete any volumes that are attached. My AWS usage exploded over night after I set this up and I was almost on the hook for a decent chunk of change for reasons beyond me. All has been quiet since I did that though.