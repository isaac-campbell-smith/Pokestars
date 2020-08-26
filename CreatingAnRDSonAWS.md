## How to Set Up an AWS Relational Database (RDS)

Since introducing this project to a number of colleagues, I was a bit surprised that the most interesting component to folks is getting this database up and running. The AWS platform can be a a pain to work with, so I get it, and I'm here for you! These are the essential steps I took to set up an RDS.

### Step 1: Get Data

OK, maybe this is an obtuse start to a step-by-step guide, but I feel it's important to note how I went about setting up a SQL database and that there are multiple ways to accomplish Step 1. I unpacked the smogon.com JSON files & assigned unique IDs across "related" DataFrames using Python's Pandas library. Check out the src folder in this repo for how that was accomplished. That code is pretty distinct to this project but will be modularized further if I do more of this in the future.

TL;DR -- Store database 'tables' as separate csv files (one for each table in your database) 

### Step 2: Transfer Data to S3 Bucket

This should be the easiest step. Create a new S3 Bucket and store your csv's. I gave mine public read access. Check out this link if you need further help:
https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-1-create-bucket.html

### Step 3: Create an RDS Instance

Navigate to the RDS section of your AWS dashboard and create a new RDS. The GUI directs you toward a lot of default settings that you don't need to tinker with much - I did this on the free tier single zone option. Just make sure you write down your database name, username and password for later. 

The important part here is how you configure the security group (you'll probably need to create a new one). Give the security group these inbound settings: <br><br>
<img src='https://raw.githubusercontent.com/isaac-campbell-smith/Pokestars/master/figures/RDS%20Security%20Settings.png' width='500'><br>

The HTTP and SSH rules allow you to interact with the EC2 instance you'll set up in the next step. The PostgreSQL rule allows you to populate the database from the Postgres app. Port range 5432 is specified because that's the standard port associated with Postgres. You'll need to tailor that rule to whatever SQL server you use. Security outbound rules can be set to 'All Traffic'. 

Once you click create, AWS will generate an Endpoint for the RDS. That address is how you'll connect to the database from your SQL software, so copy it down for later (it should look something like dbname.lqujik0xtcc6n.us-west-2.rds.amazonaws.com). Finally, take note of the security group name that is generated for your RDS. You need that for the EC2 instance you'll set up next. 

After this step you should have saved 5 items: database name, username, password, endpoint address, and security group name. 

### Step 4: Create an EC2 Instance

This step is fairly similar to the above but through the EC2 section of AWS. I chose `t2.micro` for the instance type and Ubuntu 16.04 as the operating system. You should give it an IAM role that allows it full access to S3 (which you'll need to create if you don't already have one). Also, when assigning your security group make sure it's the same group that you created above.

### Step 5: Populate RDS From EC2

My files aren't so large that I had to be careful about locally copying them to my EC2 instance. I've since written a script which incorporates a psycopg2 cursor and streams data from the S3 bucket so you don't have to store it locally. It requires about twice as much code but is, I believe, a much safer route cost-wise if you have big tables that will exceed the EC2 storage limits. Checkout the data_to_db.py file in the src folder to learn more there. If you don't have a whole lot of data you're working with, I do recommend just syncing the bucket because it's less technically demanding. I'll note where in the process the steps diverge, but first start up your EC2 instance.

Once you're in the EC2 terminal, run the following commands.<br>

<li> <code>sudo apt update
<li> sudo apt upgrade
<li> wget -S -T 10 -t 5 https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh -O $HOME/anaconda.sh
<li> bash anaconda.sh </code> (<i> answer yes to all prompts </i>)

<li> <code>source ~/.bashrc
<li> pip install awscli 
<li> pip install psycopg2</code> (<i> not necessary with local storage method </i>)
</code><br><br>

Install and verify that postgres is enabled and accepting connections at port 5432
<li> <code>sudo apt install postgresql
<li> sudo systemcl is-active postgresql
<li> sudo systemctl is-enabled postgresql
<li> sudo systemctl status postgresql
<li> sudo pg_isready
</code><br><br>

Next, you need to access your S3 bucket via your preferred method. If you're streaming the data in from a python script, you should enter `vim data_to_db.py` (or whatever name you fancy giving it) and copy in the code you've tailored for your data/base. Then run `python data_to_db.py` and scroll down to the final step as that's just about it.

If you'd rather just copy the csv's to your instance, run this line of code:
<li> <code>aws s3 sync s3://< BUCKETNAME >/ /home/ubuntu/data
</code><br><br>

Then, connect to the database with appropriate modifications to this code block:<br><br>
<code>
psql -U postgres <br>
   --host=< ENDPOINT ADDRESS > <br>
   --port=5432 <br>
   --username=< USERNAME > <br>
   --password=< PASSWORD > <br>
   --dbname= < DBNAME >
</code>
<br><br>
AND dump the csv's into tables as desired. <i> NOTE: You should be executing these commands inside of postgres!!! You terminal prompt should now look like </i> `postgres=#`. The general syntax here is: <br><br>
<code>CREATE TABLE table_name (col1 INTEGER, col2 VARCHAR);<br>
\copy table_name FROM '/home/ubuntu/data/file.csv' DELIMITER ',' CSV HEADER;</code><br><br>
Check out the make_db.sql file in the src folder for the full code. I personally couldn't be bothered with actually importing that file into my instance and running it as a command. Once you're in the database from the psql shell you can just copy-paste everything in. Shout out to Matthew Layne for a very clear tutorial regarding this part:
https://dataschool.com/learn-sql/importing-data-from-csv-in-postgresql/

### Step 6: You Did It!

And that's it! You'll want to verify that you can actually connect to and query the database from outside the instance (check out my SQL eda notebook in the notebooks folder if you need inspiration). Once you've verified that, you should shut down your EC2 instance and delete any volumes that are attached. My AWS usage exploded over night after I set this up and I was almost on the hook for a decent chunk of change for reasons beyond me. All has been quiet since I did that though.