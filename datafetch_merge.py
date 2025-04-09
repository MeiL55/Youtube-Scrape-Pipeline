import pandas as pd
import datetime as dt
import sqlite3
from sqlite3 import Error as Err
from youtube_api import YouTubeDataAPI #I used youtube-api wrapper for this, please refer to https://pypi.org/project/youtube-data-api/ for more details.
api_key = "" # Replace with your YouTube Data API key, and you can get that from https://console.cloud.google.com/apis/credentials?project=your_project_id
yt = YouTubeDataAPI(api_key)
# each keyword as filter
q_ai1 = r"(chatgpt)"
q_ai2 = r"(gpt)"
q_ai3 = r"(language model)"
q_ai4 = r"(generative ai)"
q_ai5 = r"(language ai)"
q_ai6 = r"(generative model)"
# One annoying thing: youtube_api has a certain quota for each api key, so fully automate the code (like scraping 24 months of data in one go would throw an error)
# The best way to do this is to run the code in chunks, like 1 month at a time (based on how many videos do you have in 1 months), and then merge the dataframes together.
ONEWEEK=604800
ONEMONTH=2635200
CURRTIME=1735707600 #change this to the last time you wanna scrape, please use timestamps from https://www.epochconverter.com/ to get the timestamp of the date you want to scrape.
STARTTIME = CURRTIME - 2 * ONEMONTH #change this to the first time you wanna scrape.
ENDTIME = CURRTIME - 1 * ONEMONTH #change this to the last time you wanna scrape.
VID_ARR = [] # This is the array that will store all the video ids, and then we will merge them together at the end.
current_time = ENDTIME
while current_time > STARTTIME:
    next_week = current_time - ONEWEEK
    if next_week < STARTTIME:
        next_week = STARTTIME
# Fetch YouTube search results for the given time window
    for query in [q_ai1, q_ai2, q_ai3, q_ai4, q_ai5, q_ai6]:
        results = yt.search(
            query, 
            published_before=dt.datetime.fromtimestamp(current_time), 
            published_after=dt.datetime.fromtimestamp(next_week), 
            parser=None, order='viewCount', max_results=200
        )
        print(f"Query {query}: Retrieved {len(results)} results")
        VID_ARR.extend(results)
    print(f"Retrieved {len(results)} results from {dt.datetime.fromtimestamp(next_week)} to {dt.datetime.fromtimestamp(current_time)}")
    print(f"Currently Retrieved {len(VID_ARR)} results")
    current_time = next_week
# Remove duplicates from the list of videos
unique_vids = set()
filtered_vid_arr = []
for video in VID_ARR:
    video_id = video.get("id")
    if isinstance(video_id, dict):
        video_id = str(video_id)
    if video_id not in unique_vids:
        unique_vids.add(video_id)
        filtered_vid_arr.append(video)
VID_ARR = filtered_vid_arr
print(f"After removing duplicates, {len(VID_ARR)} unique videos remain.")
df = pd.DataFrame(VID_ARR)
df['video_id'] = [x['videoId'] for x in df['id']]
df['publishedTime'] = [x['publishedAt'] for x in df['snippet']]
df['title'] = [x['title'] for x in df['snippet']]
df['description'] = [x['description'] for x in df['snippet']]
df['channelTitle'] = [x['channelTitle'] for x in df['snippet']]
df['channelId'] = [x['channelId'] for x in df['snippet']]
df = df.drop(columns=['kind', 'etag', 'id', 'snippet'], axis = 1)
df.to_csv("video.csv", index=False)
df_metadata = yt.get_video_metadata_gen(df['video_id'])
df_metadata = pd.DataFrame(df_metadata)
df_metadata = df_metadata[['video_id', 'video_category', 'video_view_count', 'video_comment_count', "video_like_count", "video_tags"]]
df_metadata.to_csv("videometa.csv", index=False)
df_channel= yt.get_channel_metadata(list(df['channelId']))
df_channel = pd.DataFrame(list(df_channel))
df_channel.to_csv("channel.csv", index=False)
# Merge the dataframes
def SQLite_connection():
 
    try:
        conn = sqlite3.connect('merge.db')
        print("Database connection is established successfully!")
        conn = sqlite3.connect(':memory:')
        print("Established database connection to a database\
        that resides in the memory!") 
        return conn

    except Err: print(Err)  
all_dfs = []

filename_video = ""
filename_videometa = ""
filename_channel = ""

for i in range(1, 26):
    # Start a new connection
    conn = sqlite3.connect(":memory:")  # Using in-memory SQLite
    filename_video = f"Jn_{i}_video.csv"
    filename_videometa = f"Jn_{i}_videometa.csv"
    filename_channel = f"Jn_{i}_channel.csv"
    filename_merge = f"merged_{i}.csv"  # Single final merged file

    # Read CSV files
    video_results = pd.read_csv(filename_video)
    video_stats = pd.read_csv(filename_videometa)
    channel_stats = pd.read_csv(filename_channel)

    # Load into SQLite
    video_results.to_sql("video_results", conn, index=False, if_exists="replace")
    channel_stats.to_sql("channel_stats", conn, index=False, if_exists="replace")
    video_stats.to_sql("video_stats", conn, index=False, if_exists="replace")

    # Query
    que = """
    SELECT 
        v.video_id,
        v.publishedTime,
        v.title,
        v.description,
        v.channelId,
        v.channelTitle,
        c.account_creation_date,
        c.keywords,
        c.description AS channel_description,
        c.view_count,
        c.subscription_count,
        c.country,
        s.video_category,
        s.video_view_count,
        s.video_comment_count,
        s.video_like_count,
        s.video_tags
    FROM video_results v
    JOIN channel_stats c ON v.channelId = c.channel_id
    JOIN video_stats s ON v.video_id = s.video_id
    """
    merged_df = pd.read_sql_query(que, conn)
    # Close connection
    conn.close()
    all_dfs.append(merged_df)
    merged_df.to_csv(filename_merge, index=False)
filename_merge = "final_merged.csv"  # Single final merged file
final_merged_df = pd.concat(all_dfs, ignore_index=True)
#remove duplicates
df_cleaned = final_merged_df.drop_duplicates(subset=['video_id'])
df_cleaned.to_csv(filename_merge, index=False)
