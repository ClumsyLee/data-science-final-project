import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import math

def calculate_chats_density(chats, video_length, sample_window_size):
    chats_density = np.zeros(math.ceil(video_length / sample_window_size))
    df = pd.DataFrame(list(map(lambda x:x['content_offset_seconds'], chats)), columns=['offset'])
    count = df.groupby(df.offset//sample_window_size).count()
    ind = np.asarray(count.index, dtype='int')
    cnt = np.asarray(count.iloc[:, 0], dtype='int')
    chats_density[ind] = cnt
    return chats_density


def calculate_active_user_density(chats, video_length, sample_window_size):
    active_user_density = np.zeros(math.ceil(video_length / sample_window_size))
    group_info = (np.array(list(map(lambda x:x['content_offset_seconds'], chats))) // sample_window_size).astype('int')
    df = pd.DataFrame(list(map(lambda x:x['commenter']['_id'], chats)), columns=['user_id'])
    for group in df.groupby(group_info):
        active_user_density[group[0]] = len(np.unique(group[1]))
    return active_user_density


def calculate_label(clips, video_length, sample_window_size):
    labels = np.zeros(math.ceil(video_length / sample_window_size))
    for clip in clips:
        for offset in range(clip.video_offset, int(clip.video_offset + clip.duration), sample_window_size):
            labels[offset // sample_window_size] = 1
    return labels


def extract_text(chats, video_length, sample_window_size):
    texts = ["" for i in range(math.ceil(video_length / sample_window_size))]
    group_info = (np.array(list(map(lambda x:x['content_offset_seconds'], chats))) // sample_window_size).astype('int')
    df = pd.DataFrame(list(map(lambda x:x['message']['body'], chats)), columns=['body'])
    for group in df.body.groupby(group_info):
        texts[group[0]] = " ".join(group[1].tolist())
    return np.asarray(texts)


def train_and_transform_tfidf(texts):
    vectorizer = TfidfVectorizer()
    features = vectorizer.fit_transform(texts)
    return vectorizer, features
