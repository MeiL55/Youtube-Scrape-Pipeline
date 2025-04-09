import pandas as pd

trans = pd.read_csv("finale.csv")
df_merge = pd.read_csv("final_merged.csv")
video_list = trans['video_id'].unique().tolist()
df = df_merge[df_merge['video_id'].isin(video_list)]
df = df.merge(trans, on='video_id', how='inner')

all_videos = df_merge['video_id'].tolist()

df.rename(columns={"transcript": "transcripts"}, inplace=True)
df.to_csv("final_all.csv", index=False)
#if you want to see videos without transcripts, uncomment the following lines
'''
no_trans = []
for id in all_videos:
    if id not in video_list:
        no_trans.append(id)
data = {'video_id': no_trans}
data = pd.DataFrame(data)
data.to_csv("no_transcripts.csv", index=False)
'''