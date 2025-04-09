
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi #We use this scraper to access to youtube video transcripts.
import time

df_merge = pd.read_csv("no_transcripts.csv")
merge_list = df_merge["video_id"].tolist()
video_id_list = merge_list
process_list = []

def transcript(json_dict):  # Convert JSON into text
    transcript = []
    for dic in json_dict:
        line = dic.get("text", "")
        if line and line != "[Music]":
            transcript.append(line)
    return " ".join(transcript).replace("\n", "")

def get_list_trans(video_ids):  # Get transcripts for a list of videos
    transcript_list = []
    process_list = []
    total_videos = len(video_ids)

    batch_size = 500
    for batch_start in range(0, total_videos, batch_size):
        batch = video_ids[batch_start:batch_start + batch_size]
        print(f"\nStarting batch {batch_start + 1} to {batch_start + len(batch)}...\n")

        for index, video_id in enumerate(batch, start=batch_start + 1):
            try:
                dic = YouTubeTranscriptApi.get_transcript(video_id)
                process_list.append(video_id)
                transcript_list.append(dic)
                print(f"Fetched transcript for video {index}/{total_videos}: {video_id}")

            except Exception as e:
                print(f"Skipping {index}/{total_videos}: {video_id}")
                continue  # Skip to the next video if there's an error

            # Print progress every 10%
            if index % max(1, total_videos // 10) == 0 or index == total_videos:
                progress = (index / total_videos) * 100
                print(f"Progress: {progress:.1f}% ({index}/{total_videos} videos processed)")

        # Pause before the next batch
        if batch_start + batch_size < total_videos:
            print(f"Sleeping for 600 seconds before next batch...\n")
            time.sleep(600) # Sleep for 10 minutes to avoid instability caused by rate limits
    return transcript_list, process_list


# Execute transcript fetching with error handling
try:
    list_of_dic, process_list = get_list_trans(video_id_list)
except Exception as e:
    print(f"Unexpected error: {e}")

# Convert each transcript JSON to text
transcript_list = []
for index, transcript_data in enumerate(list_of_dic, start=1):
    print(f"Processing {index}/{len(list_of_dic)} transcripts...")
    single = transcript(transcript_data)
    transcript_list.append(single)

print(f"All {len(transcript_list)} transcripts processed!")

# Create a DataFrame with results
df = pd.DataFrame({
    "video_id": process_list,
    "transcript": transcript_list
})
print("DataFrame created successfully!")
df.to_csv("test.csv", index=False)