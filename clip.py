from time import sleep

import requests

ROOT_URL = 'https://api.twitch.tv/kraken'
CLIENT_ID = 'jzkbprff40iqj646a697cyrvl0zt2m6'


class Clip(object):
    """A clip for Twitch.tv."""

    def __init__(self, slug, duration, views, video_id, video_offset):
        self.slug = slug
        self.duration = duration
        self.views = views
        self.video_id = video_id
        self.video_offset = video_offset

    @staticmethod
    def get_top(channel, period='week'):
        clips = []
        slugs = set()
        video_ids = set()

        url = ROOT_URL + '/clips/top'
        params = {
            'channel': channel,
            'limit': 100,
            'period': period  # day, week, month, all
        }
        headers = {
            'Accept': 'application/vnd.twitchtv.v5+json',
            'Client-Id': CLIENT_ID
        }
        has_more = True

        while has_more:
            response = requests.get(url, params=params, headers=headers)
            json = response.json()

            if len(json['clips']) == 0:
                has_more = False

            for clip_json in json['clips']:
                # Stop when the loop begins.
                slug = clip_json['slug']
                if slug in slugs:
                    has_more = False
                    break
                slugs.add(slug)

                # Skip clips that don't have vods.
                vod_json = clip_json['vod']
                if vod_json is None:
                    continue

                duration = clip_json['duration']
                views = clip_json['views']
                video_id = vod_json['id']
                video_offset = vod_json['offset']

                clip = Clip(slug=slug, duration=duration, views=views,
                            video_id=video_id, video_offset=video_offset)
                clips.append(clip)

                video_ids.add(video_id)

            # Prepare for the next page.
            params['cursor'] = json['_cursor']
            print(f'Got {len(clips)} clips from {len(video_ids)} videos.')
            sleep(2.0)

        return clips


if __name__ == '__main__':
    channel = 'overwatchleague'
    clips = Clip.get_top(channel)
    print(f'Got {len(clips)} clips from the channel {channel}.')
