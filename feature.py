import pickle
import sys

import numpy as np
from sklearn import feature_extraction


def offset_to_i_window(offset, window_length):
    return int(offset // window_length)


def split_chats(chats, window_num, window_length):
    text_windows = [[] for _ in range(window_num)]

    for chat in chats:
        i_window = offset_to_i_window(chat['content_offset_seconds'],
                                      window_length)
        if i_window >= window_num:
            continue

        text = chat['message']['body']
        text_windows[i_window].append(text)

    texts = ['\n'.join(window) for window in text_windows]
    return texts


def split_clips(clips, window_num, window_length):
    labels = -1 * np.ones(window_num)

    for clip in clips:
        i_window_from = offset_to_i_window(clip.video_offset,
                                           window_length)
        i_window_to = offset_to_i_window(clip.video_offset + clip.duration,
                                         window_length) + 1

        if i_window_to > window_num:
            continue

        for i_window in range(i_window_from, i_window_to):
            labels[i_window] = 1

    return labels


def split_windows(chats, clips, video_length, window_length=10.0):
    window_num = offset_to_i_window(video_length, window_length) + 1

    texts = split_chats(chats, window_num, window_length)
    labels = split_clips(clips, window_num, window_length)

    return texts, labels


def train_tfidf(texts):
    vectorizer = feature_extraction.text.TfidfVectorizer(stop_words='english')
    features = vectorizer.fit_transform(texts)

    return vectorizer, features


if __name__ == '__main__':
    video_id = sys.argv[1]

    video = pickle.load(open(f'videos/{video_id}.pickle', 'rb'))
    clips = pickle.load(open('clips.pickle', 'rb'))

    texts, labels = split_windows(video['chats'], clips, video['length'])
    vectorizer, features = train_tfidf(texts)

    print(labels)
    print(features)
