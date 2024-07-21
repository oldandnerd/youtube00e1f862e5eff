import re
from typing import AsyncGenerator
import aiohttp
import dateparser
import time
import asyncio
import requests
import random
import json
from bs4 import BeautifulSoup
from typing import AsyncGenerator
from datetime import datetime
from exorde_data import (
    Item,
    Content,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId
)
import logging
from aiohttp_socks import ProxyConnector

try:
    import nltk
    nltk.download('stopwords')
    stopwords = nltk.corpus.stopwords.words('english')
except Exception as e:
    logging.exception(f"[Youtube] nltk.corpus.stopwords.words('english') error: {e}")
    stopwords = []

MAX_TOTAL_COMMENTS_TO_CHECK = 150
PROBABILITY_ADDING_SUFFIX = 0.85
PROBABILITY_DEFAULT_KEYWORD = 0.4

DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 50
DEFAULT_MIN_POST_LENGTH = 10

DEFAULT_KEYWORDS = [
    # [.. the list of default keywords ...]
]

global YT_COMMENT_DLOADER_
YT_COMMENT_DLOADER_ = None

NB_AJAX_CONSECUTIVE_MAX_TRIALS = 15
REQUEST_TIMEOUT = 8
POST_REQUEST_TIMEOUT = 4
SORT_BY_POPULAR = 0
SORT_BY_RECENT = 1

YOUTUBE_VIDEO_URL = 'https://www.youtube.com/watch?v={youtube_id}'
YOUTUBE_CONSENT_URL = 'https://consent.youtube.com/save'

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

SORT_BY_POPULAR = 0
SORT_BY_RECENT = 1

YT_CFG_RE = r'ytcfg\.set\s*\(\s*({.+?})\s*\)\s*;'
YT_INITIAL_DATA_RE = r'(?:window\s*\[\s*["\']ytInitialData["\']\s*\]|ytInitialData)\s*=\s*({.+?})\s*;\s*(?:var\s+meta|</script|\n)'
YT_HIDDEN_INPUT_RE = r'<input\s+type="hidden"\s+name="([A-Za-z0-9_]+)"\s+value="([A-Za-z0-9_\-\.]*)"\s*(?:required|)\s*>'

class YoutubeCommentDownloader:

    def __init__(self, proxy_url):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = USER_AGENT
        self.session.cookies.set('CONSENT', 'YES+cb', domain='.youtube.com')
        self.proxy_url = proxy_url

    def ajax_request(self, endpoint, ytcfg, retries=5, sleep=15):
        url = 'https://www.youtube.com' + endpoint['commandMetadata']['webCommandMetadata']['apiUrl']

        data = {'context': ytcfg['INNERTUBE_CONTEXT'],
                'continuation': endpoint['continuationCommand']['token']}

        for _ in range(retries):
            response = self.session.post(url, params={'key': ytcfg['INNERTUBE_API_KEY']}, json=data, timeout=POST_REQUEST_TIMEOUT, proxies={"http": self.proxy_url, "https": self.proxy_url})
            if response.status_code == 200:
                return response.json()
            if response.status_code in [403, 413]:
                return {}
            else:
                time.sleep(sleep)

    def get_comments(self, youtube_id, *args, **kwargs):
        return self.get_comments_from_url(YOUTUBE_VIDEO_URL.format(youtube_id=youtube_id), *args, **kwargs)

    def get_comments_from_url(self, youtube_url, sort_by=SORT_BY_RECENT, language=None, sleep=.1, limit=100, max_oldness_seconds=3600):
        response = self.session.get(youtube_url, timeout=REQUEST_TIMEOUT, proxies={"http": self.proxy_url, "https": self.proxy_url})

        if 'consent' in str(response.url):
            params = dict(re.findall(YT_HIDDEN_INPUT_RE, response.text))
            params.update({'continue': youtube_url, 'set_eom': False, 'set_ytc': True, 'set_apyt': True})
            response = self.session.post(YOUTUBE_CONSENT_URL, params=params, timeout=REQUEST_TIMEOUT, proxies={"http": self.proxy_url, "https": self.proxy_url})

        html = response.text
        ytcfg = json.loads(self.regex_search(html, YT_CFG_RE, default=''))
        if not ytcfg:
            return
        if language:
            ytcfg['INNERTUBE_CONTEXT']['client']['hl'] = language

        data = json.loads(self.regex_search(html, YT_INITIAL_DATA_RE, default=''))

        item_section = next(self.search_dict(data, 'itemSectionRenderer'), None)
        renderer = next(self.search_dict(item_section, 'continuationItemRenderer'), None) if item_section else None
        if not renderer:
            return

        sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu:
            section_list = next(self.search_dict(data, 'sectionListRenderer'), {})
            continuations = list(self.search_dict(section_list, 'continuationEndpoint'))
            data = self.ajax_request(continuations[0], ytcfg) if continuations else {}
            sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu or sort_by >= len(sort_menu):
            raise RuntimeError('Failed to set sorting')
        continuations = [sort_menu[sort_by]['serviceEndpoint']]

        comment_counter = 0
        old_comment_counter = 0
        break_condition = False
        while continuations:
            if break_condition:
                break

            continuation = continuations.pop()
            response = self.ajax_request(continuation, ytcfg)

            if not response:
                break

            error = next(self.search_dict(response, 'externalErrorMessage'), None)
            if error:
                raise RuntimeError('Error returned from server: ' + error)

            actions = list(self.search_dict(response, 'reloadContinuationItemsCommand')) + \
                      list(self.search_dict(response, 'appendContinuationItemsAction'))
            for action in actions:
                for item in action.get('continuationItems', []):
                    if action['targetId'] in ['comments-section',
                                              'engagement-panel-comments-section',
                                              'shorts-engagement-panel-comments-section']:
                        continuations[:0] = [ep for ep in self.search_dict(item, 'continuationEndpoint')]
                    if action['targetId'].startswith('comment-replies-item') and 'continuationItemRenderer' in item:
                        continuations.append(next(self.search_dict(item, 'buttonRenderer'))['command'])

            toolbar_payloads = self.search_dict(response, 'engagementToolbarStateEntityPayload')
            toolbar_states = {payloads['key']: payloads for payloads in toolbar_payloads}
            for comment in reversed(list(self.search_dict(response, 'commentEntityPayload'))):
                properties = comment['properties']
                author = comment['author']
                toolbar = comment['toolbar']
                toolbar_state = toolbar_states[properties['toolbarStateKey']]
                result = {'cid': properties['commentId'],
                          'text': properties['content']['content'],
                          'time': properties['publishedTime'],
                          'author': author['displayName'],
                          'channel': author['channelId'],
                          'votes': toolbar['likeCountLiked'],
                          'replies': toolbar['replyCount'],
                          'photo': author['avatarThumbnailUrl'],
                          'heart': toolbar_state.get('heartState', '') == 'TOOLBAR_HEART_STATE_HEARTED',
                          'reply': '.' in properties['commentId']}

                try:
                    result['time_parsed'] = dateparser.parse(result['time'].split('(')[0].strip()).timestamp()
                except AttributeError:
                    pass

                paid = (
                    comment.get('paidCommentChipRenderer', {})
                    .get('pdgCommentChipRenderer', {})
                    .get('chipText', {})
                    .get('simpleText')
                )
                if paid:
                    result['paid'] = paid

                comment_counter += 1
                if comment_counter >= limit:
                    logging.info(f"[Youtube] Comment limit reached: {limit} newest comments found. Moving on...")
                    break_condition = True
                    break
                if result['time_parsed'] < time.time() - max_oldness_seconds:
                    old_comment_counter += 1
                    if old_comment_counter > 10:
                        logging.info(f"[Youtube] The most recent comments are too old, moving on...")
                        break_condition = True
                    break

                yield result
            time.sleep(sleep)

    @staticmethod
    def regex_search(text, pattern, group=1, default=None):
        match = re.search(pattern, text)
        return match.group(group) if match else default

    @staticmethod
    def search_dict(partial, search_key):
        stack = [partial]
        while stack:
            current_item = stack.pop()
            if isinstance(current_item, dict):
                for key, value in current_item.items():
                    if key == search_key:
                        yield value
                    else:
                        stack.append(value)
            elif isinstance(current_item, list):
                stack.extend(current_item)


def is_within_timeframe_seconds(input_timestamp, timeframe_sec):
    input_timestamp = int(input_timestamp)
    current_timestamp = int(time.time())
    elapsed_time = current_timestamp - input_timestamp

    if elapsed_time <= timeframe_sec:
        return True
    else:
        return False
    
def extract_url_parts(urls):
    result = []
    for url in urls:
        url_part = url.split('&')[0]
        result.append(url_part)
    return result

def convert_timestamp(timestamp):
    dt = datetime.utcfromtimestamp(int(timestamp))
    formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    return formatted_dt

def randomly_add_search_filter(input_URL, p):
    suffixes_dict = {
        "&sp=CAI%253D": "newest videos",
        "&sp=CAASAhAB": "relevant videos",
        "&sp=EgQIARAB": "last hour videos",
        "&sp=EgQIAhAB": "last day videos",
        "&sp=EgQIAxAB": "last week videos",
    }
    suffixes_list = list(suffixes_dict.keys())
    if random.random() < p:
        chosen_suffix = random.choices(suffixes_list, weights=[0.10, 0.15, 0.25, 0.25, 0.25])[0]
        logging.info(f"[Youtube] Adding search filter to URL:  {suffixes_dict[chosen_suffix]}")
        return input_URL + chosen_suffix
    else:
        return input_URL
    
async def scrape(keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check, proxy_url):
    global YT_COMMENT_DLOADER_
    URL = "https://www.youtube.com/results?search_query={}".format(keyword)
    URL = randomly_add_search_filter(URL, p=PROBABILITY_ADDING_SUFFIX)
    logging.info(f"[Youtube] Looking at video URL: {URL}")

    connector = ProxyConnector.from_url(proxy_url)
    async with aiohttp.ClientSession(headers={'User-Agent': USER_AGENT}, connector=connector) as session:
        try:
            async with session.get(URL, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                html = await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"An error occurred during the request: {e}")
            return

    soup = BeautifulSoup(html, 'html.parser')

    URLs_remaining_trials = 10
    await asyncio.sleep(2)
    script_tag = soup.find('script', string=lambda text: text and 'var ytInitialData' in text)

    urls = []
    titles = []
    if script_tag:
        await asyncio.sleep(0.1)
        json_str = str(script_tag)
        start_index = json_str.find('var ytInitialData = ') + len('var ytInitialData = ')
        end_index = json_str.rfind('};') + 1
        json_data_str = json_str[start_index:end_index]

        try:
            data = json.loads(json_data_str)
            if 'contents' in data:
                logging.info("[Youtube] Parsing search page: raw contents found...")
                primary_contents = data['contents']['twoColumnSearchResultsRenderer']['primaryContents']
                for item in primary_contents['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents']:
                    if 'videoRenderer' in item:
                        video = item['videoRenderer']
                        title = video['title']['runs'][0]['text']
                        url_suffix = video['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url']
                        full_url = f"https://www.youtube.com{url_suffix}"
                        urls.append(full_url)
                        titles.append(title)
                        logging.info(f"[Youtube] Video URL found = {full_url} and title: {title}")

        except json.JSONDecodeError as e:
            logging.info(f"[Youtube] Invalid JSON data in var ytInitialData: {e}")
    else:
        logging.info("[Youtube] No ytInitialData found.")

    last_n_video_comment_count = []
    n_rolling_size = 8
    n_rolling_size_min = 3

    yielded_items = 0
    nb_comments_checked = 0

    urls = extract_url_parts(urls)
    try:
        urlstitles = list(zip(urls, titles))
        random.shuffle(urlstitles)
        urls, titles = zip(*urlstitles)
    except Exception as e:
        if len(urls) == 0 or len(titles) == 0:
            logging.info(f"[Youtube] urls or titles is empty, skipping...")
        else:
            logging.exception(f"[Youtube] zip(*urlstitles) error: {e}")
        return
    
    for url, title in zip(urls, titles):
        await asyncio.sleep(1)
        if random.random() < 0.1:
            logging.info(f"[Youtube] Randomly skipping URL: {url}")
            continue
        youtube_video_url = url
        comments_list = []
        nb_zeros = 0
        if len(last_n_video_comment_count) >= n_rolling_size_min:
            for i in range(len(last_n_video_comment_count)-1, -1, -1):
                if last_n_video_comment_count[i] == 0:
                    nb_zeros += 1
                else:
                    break
            random_inter_sleep = round(0.1 + nb_zeros*0.2, 1)
            logging.info(f"[Youtube] [soft rate limit] Waiting  {random_inter_sleep} seconds...")
            await asyncio.sleep(random_inter_sleep)

        try:
            logging.info(f"[Youtube] Getting ...{url}")
            comments_list = YT_COMMENT_DLOADER_.get_comments_from_url(url, sort_by=SORT_BY_RECENT, max_oldness_seconds=max_oldness_seconds)

            comments_list = list(comments_list)
            nb_comments = len(comments_list)
            nb_comments_checked += nb_comments
            logging.info(f"[Youtube] Found {nb_comments} recent comments on video: {title}")
            last_n_video_comment_count.append(len(comments_list))
            if len(last_n_video_comment_count) > n_rolling_size:
                last_n_video_comment_count.pop(0)
            if len(last_n_video_comment_count) == n_rolling_size:
                if sum(last_n_video_comment_count) == 0:
                    logging.info("[Youtube] [RATE LIMITE PROTECTION] The rolling window of comments count is full of 0s. Stopping the scraping iteration...")
                    break
        except Exception as e:
            logging.exception(f"[Youtube] YT_COMMENT_DLOADER_ - ERROR: {e}")
            random_inter_sleep = round(3 + random.random()*7, 1)
            logging.info(f"[Youtube] Waiting  {random_inter_sleep} seconds after the error...")
            await asyncio.sleep(random_inter_sleep)

        for comment in comments_list:
            try:
                comment_timestamp = int(round(comment['time_parsed'], 1))
            except Exception as e:
                logging.exception(f"[Youtube] parsing comment datetime error: {e}\n \
                THIS CAN BE DUE TO FOREIGN/SPECIAL DATE FORMAT, not handled at this date.\n Please report this to the Exorde discord, with your region/VPS location.")

            comment_url = youtube_video_url + "&lc=" + comment['cid']
            comment_id = comment['cid']
            if len(comment['text']) < 5:
                continue
            try:
                title_base = " ".join([word for word in title.split(" ") if word not in stopwords])
                titled_context = title_base
            except Exception as e:
                logging.exception(f"[Youtube] stopwords error: {e}")
                titled_context = title
            if random.random() < 0.3:
                titled_context = " ".join([word for word in title.split(" ") if random.random() > 0.3])
            elif random.random() < 0.4:
                titled_context = " ".join([word for word in title.split(" ") if random.random() > 0.2])
            titled_context = " ".join([word for word in titled_context.split(" ") if word.isalnum() and len(word) > 1])
            comment_content = titled_context + ". " + comment['text']
            comment_datetime = convert_timestamp(comment_timestamp)
            if is_within_timeframe_seconds(comment_timestamp, max_oldness_seconds):
                comment_obj = {'url': comment_url, 'content': comment_content, 'title': title, 'created_at': comment_datetime, 'external_id': comment_id}
                logging.info(f"[Youtube] found new comment: {comment_obj}")
                yield Item(
                    content=Content(str(comment_content)),
                    created_at=CreatedAt(str(comment_obj['created_at'])),
                    title=Title(str(comment_obj['title'])),
                    domain=Domain("youtube.com"),
                    url=Url(comment_url),
                    external_id=ExternalId(str(comment_obj['external_id']))
                )
                yielded_items += 1
                if yielded_items >= maximum_items_to_collect:
                    break
        if nb_comments_checked >= max_total_comments_to_check:
            break
        
        URLs_remaining_trials -= 1
        if URLs_remaining_trials <= 0:
            break
            
def randomly_replace_or_choose_keyword(input_string, p):
    if random.random() < p:
        return input_string
    else:
        return random.choice(DEFAULT_KEYWORDS)

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

        try:
            probability_to_select_default_kws = parameters.get("probability_to_select_default_kws", PROBABILITY_DEFAULT_KEYWORD)
        except KeyError:
            probability_to_select_default_kws = PROBABILITY_DEFAULT_KEYWORD

        try:
            max_total_comments_to_check = parameters.get("max_total_comments_to_check", MAX_TOTAL_COMMENTS_TO_CHECK)
        except KeyError:
            max_total_comments_to_check = MAX_TOTAL_COMMENTS_TO_CHECK

    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        probability_to_select_default_kws = PROBABILITY_DEFAULT_KEYWORD
        max_total_comments_to_check = MAX_TOTAL_COMMENTS_TO_CHECK

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, probability_to_select_default_kws, max_total_comments_to_check


def convert_spaces_to_plus(input_string):
    return input_string.replace(" ", "+")

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    global YT_COMMENT_DLOADER_
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length, probability_to_select_default_kws, max_total_comments_to_check  = read_parameters(parameters)
    selected_keyword = ""
    proxy_url = parameters.get("proxy_url", "socks5://2607:f130:0:f8::2bab:3a16:1080")
    YT_COMMENT_DLOADER_ = YoutubeCommentDownloader(proxy_url)
    
    content_map = {}
    await asyncio.sleep(1)
    try:
        if "keyword" in parameters:
            selected_keyword = parameters["keyword"]
        selected_keyword = randomly_replace_or_choose_keyword(selected_keyword, p=probability_to_select_default_kws)
        selected_keyword = convert_spaces_to_plus(selected_keyword)
    except Exception as e:
        logging.exception(f"[Youtube parameters] parameters: {parameters}. Error when reading keyword: {e}")        
        selected_keyword = randomly_replace_or_choose_keyword("", p=1)

    logging.info(f"[Youtube] - Scraping latest comments posted less than {max_oldness_seconds} seconds ago, on youtube videos related to keyword: {selected_keyword}.")
    try:
        async for item in scrape(selected_keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check, proxy_url):
            if item['content'] in content_map:
                continue
            else:
                content_map[item['content']] = True
            if len(item['content']) < min_post_length:
                continue
            yielded_items += 1
            yield item
            if yielded_items >= maximum_items_to_collect:
                break
    except asyncio.exceptions.TimeoutError:
        logging.info(f"[Youtube] Internal requests are taking longer than {REQUEST_TIMEOUT} - we must give up & move on. Check your network.")

