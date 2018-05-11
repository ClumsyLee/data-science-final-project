import requests
from multiprocessing import Pool
import math
import logging
import os
import pickle
import clip

PROCESS_NUM = 20
RETRY_TIME = 5
MAGIC_CLIENT_ID = "jzkbprff40iqj646a697cyrvl0zt2m6"


def init_directory_structure():
    if not os.path.exists('data'):
        os.path.mkdir('data')
    if not os.path.exists('data/chats'):
        os.path.mkdir('data/chats')
    if not os.path.exists('data/clips'):
        os.path.mkdir('data/chats')
    if not os.path.exists('data/videos'):
        os.path.mkdir('data/chats')


def twitch_get_video_info(video_id):
    cnt = 0
    while True:
        try:
            cnt += 1
            req = requests.get("https://api.twitch.tv/helix/videos", headers={"client-id":MAGIC_CLIENT_ID}, params={"id":video_id} )
            break
        except:
            if cnt > RETRY_TIME:
                raise
    return req.json()['data'][0]


def duration_str_to_seconds(d_str):
    ans = 0
    tmp = d_str.split('h')
    if len(tmp) > 1:
        ans += int(tmp[0]) * 3600
        d_str = tmp[1]

    tmp = d_str.split('m')
    if len(tmp) > 1:
        ans += int(tmp[0]) * 60
        d_str = tmp[1]

    tmp = d_str.split('s')
    if len(tmp) > 1:
        ans += int(tmp[0])

    return ans


def twitch_crawl_chat_in_interval(args):
    video_id, start, end = args
    url = "https://api.twitch.tv/v5/videos/%s/comments" % video_id
    cnt = 0
    while True:
        try:
            cnt +=1
            req = requests.get(url, headers={"client-id":MAGIC_CLIENT_ID}, params={"content_offset_seconds":start})
            break
        except:
            if cnt > RETRY_TIME:
                raise
    comments = []
    while True:
        if req.status_code != 200:
            logging.error(req.text)
            raise
        json_data = req.json()
        if len(json_data['comments']) == 0:
            break
        comments += json_data['comments']
        if comments[-1]['content_offset_seconds'] >= end or not '_next' in json_data:
            break
        cursor_str = json_data['_next']
        cnt = 0
        while True:
            try:
                cnt += 1
                req = requests.get(url, headers={"client-id":MAGIC_CLIENT_ID}, params={"cursor":cursor_str})
                break
            except:
                if cnt > RETRY_TIME:
                    raise
    if len(comments) > 0:
        n = 0
        while n < len(comments) and comments[-n-1]['content_offset_seconds'] >= end:
            n += 1
        if n > 0:
            comments = comments[:-n]
        n = 0
        while n < len(comments) and comments[n]['content_offset_seconds'] < start:
            n += 1
        comments = comments[n:]
    return comments


def twitch_crawl_chat(video_id):
    video_info = twitch_get_video_info(video_id)
    video_length = duration_str_to_seconds(video_info['duration'])
    step = math.ceil(video_length / PROCESS_NUM)
    pool = Pool(PROCESS_NUM)
    chats = pool.map(twitch_crawl_chat_in_interval, [(video_id, s, s+step) for s in range(0, video_length, step)])
    pool.close()
    pool.join()
    chats = sum(chats, [])
    return chats


def twitch_get_user_profile(login_name):
    cnt = 0
    while True:
        try:
            cnt += 1
            req = requests.get("https://api.twitch.tv/helix/users/", headers={"client-id":MAGIC_CLIENT_ID}, params={"login":login_name})
            break
        except:
            if cnt > RETRY_TIME:
                raise
    return req.json()['data'][0]


def twitch_get_user_clips(user_id):
    params = {"broadcaster_id":user_id, "first":100}
    clips = []
    while True:
        cnt = 0
        while True:
            try:
                cnt += 1
                req = requests.get("https://api.twitch.tv/helix/clips/", headers={"client-id":MAGIC_CLIENT_ID}, params = params)
                break
            except:
                if cnt > RETRY_TIME:
                    raise
        json_data = req.json()
        clips += json_data['data']
        if 'cursor' in json_data["pagination"]:
            cursor = json_data["pagination"]['cursor']
            params['after'] = cursor
        else:
            break
    return clips


def twitch_get_game_info(game_id_list):
    cnt = 0
    while True:
        try:
            cnt += 1
            req = requests.get("https://api.twitch.tv/helix/games/", headers={"client-id":MAGIC_CLIENT_ID}, params={"id":game_id_list})
            break
        except:
            if cnt > RETRY_TIME:
                raise
    return req.json()['data']


def load_chats(video_id):
    chats_file_path = 'data/chats/%s.pickle' % video_id
    if not os.path.exists(chats_file_path):
        logging.warning("Chats of %s don't exist, downloading..." % video_id)
        chats = twitch_crawl_chat(video_id)
        with open(chats_file_path, 'wb') as fout:
            pickle.dump(chats, fout)
    else:
        with open(chats_file_path, 'rb') as fin:
            chats = pickle.load(fin)
    return chats


def load_clips(user_id):
    clips_file_path = 'data/clips/%s.pickle' % user_id
    if not os.path.exists(clips_file_path):
        logging.warning("Clips of %s don't exist, downloading..." % user_id)
        clips = clip.Clip.get_top(user_id)
        with open(clips_file_path, 'wb') as fout:
            pickle.dump(clips, fout)
    else:
        with open(clips_file_path, 'rb') as fin:
            clips = pickle.load(fin)
    return clips


def load_video_infos(user_id):
    username = user_id
    user_profile = twitch_get_user_profile(user_id)
    user_id = user_profile['id']
    video_infos_path = "data/videos/%s.pickle" % user_id
    if not os.path.exists(video_infos_path):
        logging.warning("Video infos of %s don't exist, downloading..." % username)
        params = {"user_id":user_id, "first":100}
        video_infos = []
        while True:
            cnt = 0
            while True:
                try:
                    cnt += 1
                    req = requests.get("https://api.twitch.tv/helix/videos", headers={"client-id":MAGIC_CLIENT_ID}, params = params)
                    break
                except:
                    if cnt > RETRY_TIME:
                        raise
            json_data = req.json()
            if len(json_data['data']) == 0:
                break
            video_infos += json_data['data']
            if 'cursor' in json_data["pagination"]:
                cursor = json_data["pagination"]['cursor']
                params['after'] = cursor
            else:
                break
        with open(video_infos_path, 'wb') as fout:
            pickle.dump(video_infos, fout)
    else:
        with open(video_infos_path, 'rb') as fin:
            video_infos = pickle.load(fin)
    return video_infos
