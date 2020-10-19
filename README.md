## Talent Map
Insight Data Engineering Fellowship Project San Francisco 2020C Session

<a href="https://docs.google.com/presentation/d/1UsXbqKeRogs3hbmVc5Wjg2klLq2HJ3mYCNAJhjxRNrQ/edit#slide=id.g96abc2b0f8_0_58">Slides</a> 
<a href="https://youtu.be/aAGWPE6vkco">Demo</a> 


## Motivation 
Talent shortage is a real issue in the current market. Almost three quarters (72.8%) of employers are having a difficult time finding skilled candidates and More than 73% of job seekers today are only passively looking for a job. Remote work is becoming a new norm due to ongoing pandemic. It would be interesting to build a dashboard where companies can find the experienced resources across the globe for a particlar technology. Also how cool it would be to see how the technology is evolving over the years. 


## Pipeline 
![](Images/TechStack.png)

## Architechture
### Spark
4 EC2 m5ad.2xlarge instances (1 master 3 slaves spark cluster)

<a href="https://blog.insightdatascience.com/simply-install-spark-cluster-mode-341843a52b88">Installation</a> 

### PostgreSQL
1 EC2 m5ad.xlarge instance

<a href="https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252">Installation</a>

### Dash
1 EC2 m5ad.large instance

<a href="https://dash.plotly.com/installation">Installation</a>

## DataSet
StackExchange data dump. <a href="https://archive.org/download/stackexchange">DataSet Link</a> 

## Metrics
For a given tag the dashboard displays:

1) Users having expertise in that particular tech across the globe . Their reputation and any url or contact information.

2) How the questions are evolving for a particular tag evolving over the years.

## DashBoard
![](Images/dashboard.png)

## Methodology

### Data Collection
retrieve_urls.py: Uses the BeautifulSoup package to parse the urls on the stackexchange data dump to retrieve the urls of the .7z files of all the stackoverflow data.

download.sh: This script is redirect the archives in the urls of the txt file to the EC2 instance 

extract_EC2_s3.sh: unzip the xml .7z compressed files and stores them on the S3 bucket. 

### Batch GeoCoding
Batch Geocoding is done using Nominatim which has No limit with address geocoding.Nominatim doesn’t like bulk Geocoding and Repeated queries for the same address is blocked. To solve this issue , User address approx 4 million was cleaned using regex UDF and duplicate records were removed and the address's to be queried were reduced from 3 million rows to 215 K rows. Location were passed to API and GeoCoordinates were retrieved.

### Tags DataWrangling
It was required to count the answers each user is giving for a particular tag. But posts XML file (Data Dump file for Questions and Answers) , has tags only for the posttypeid = 1 . Posttypeid 1 refers to questions and 2 refers to answers. Answers were related to questions by parentid . Post dataframe was partitioned into 2 , one for questions and another for answers . Both dataframes were joined and tags were mapped to answers. Also Tags column required to be formatted so that user can be mapped to individual tags.

Steps followed were

1) The posts.xml files are read in as a dataframe.

2) The questions are stored in a separate dataframe

   df_posts_answers = df_posts_answers.where((col("post_ans_type_id") == 2 ) & (col("post_ans_user_id").isNotNull()) ) 
 
3) The answers are stored in a separate dataframe
 
   df_posts_question = df_posts_question.where((col("post_tags").isNotNull() ) & (col("post_ques_type_id") == 1 ) )

4)  Answers and questions were joined together
   df_posts_ans_tags = df_posts_answers.join(df_posts_question,df_posts_question.post_ques_id == df_posts_answers.parent_question_id,"inner")
5) df_posts_expl = df_posts.select(df_posts.post_ans_id,df_posts.post_ans_user_id,F.explode(df_posts.tag3).alias("tags_distinct"))
    window = Window.partitionBy("post_ans_user_id","tags_distinct")
    df_final =          df_posts_expl.select(df_posts_expl.post_ans_user_id,df_posts_expl.tags_distinct,F.count(df_posts_expl.post_ans_id).over(window).alias("total_count")).distinct()
    
### User Location mapped to Tags.
Finally User along with location was mapped to the tags .


    
 





