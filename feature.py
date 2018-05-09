import pickle

from sklearn import feature_extraction


def chat_to_i_window(chat, window_size):
    return int(chat['content_offset_seconds'] // window_size)


def tfidf(chats, window_size=10.0):
    window_num = chat_to_i_window(chats[-1], window_size) + 1
    windows = [[] for _ in range(window_num)]

    for chat in chats:
        i_window = chat_to_i_window(chat, window_size)
        text = chat['message']['body']

        windows[i_window].append(text)

    documents = ['\n'.join(window) for window in windows]

    vectorizer = feature_extraction.text.TfidfVectorizer(stop_words='english')
    features = vectorizer.fit_transform(documents)

    return vectorizer, features


if __name__ == '__main__':
    chats = pickle.load(open('chats.pickle', 'rb'))
    vectorizer, features = tfidf(chats)

    print(vectorizer, features)
