import atexit
from typing import List
from concurrent.futures import ThreadPoolExecutor

from lxml.html import parse
from lxml import etree, cssselect

import json
from csv import DictWriter

import urllib3
from urllib3 import PoolManager
from urllib3.response import HTTPResponse
from urllib3.exceptions import ReadTimeoutError, NewConnectionError, MaxRetryError
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)




class Connection:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0'
    }

    def __init__(self, timeout: float = 5.0, num_pools: int = 50) -> None:
        self.http = PoolManager(num_pools=num_pools, timeout=timeout)

    def get_connection(self, url: str) -> HTTPResponse:
        try:
            r = self.http.request('GET', url, preload_content=False, headers=self.headers)
            if r.status == 200:
                return r
            else:
                pass#print("Code", r.status, "at", url)
        except (NewConnectionError, MaxRetryError, ReadTimeoutError) as e:
            pass#print(e.__class__.__name__, "at", url)


class PageParser:
    def __init__(self, tags: List[str]):
        self.tags = dict.fromkeys(tags, None)

    def __parse_tags(self, source):
        context = etree.iterparse(source, events=('end',), tag=["meta"], html=True)
        for _, tag in context:
            attrs = tag.attrib
            if "property" in attrs and attrs["property"] in self.tags:
                self.tags[attrs["property"]] = attrs["content"]

            if all(self.tags.values()):
                return True
        return False

    def get_og_tags(self, r: HTTPResponse):
        atexit.register(lambda: r.release_conn())
        # TODO: add try catch block
        if not self.__parse_tags(r):
            return None # TODO: log incomplete parsing
        return self.tags

    @staticmethod
    def get_all_link(r: HTTPResponse, pattern: str = None, filter: str = ""): # TODO: add filter string to filter out link to twitter and such kind of shit
        atexit.register(lambda: r.release_conn())
        dom = parse(r).getroot()
        cls = "a"
        if pattern:
            cls = "a[class='{0}']".format(pattern)

        for link in dom.cssselect(cls):
            url = link.get('href')
            yield url


class Saver:
    def __init__(self, name: str, field_names):
        self.name = name
        self.file = open(self.name, 'w')

        self.writer = DictWriter(self.file, fieldnames=field_names)
        self.writer.writeheader()

    def save(self, row: dict):
        try:
            self.writer.writerow(row)
        except UnicodeEncodeError:
            pass #print("Can't encode message")  # TODO: log parsing error. Even better would be if we could preprocess string

    def __del__(self):
        self.file.close()


class Topic:
    def __init__(self, title: str, code: str):
        self.title = title
        self.code = code


class Crawler:
    topic_pattern = "https://techcrunch.com/wp-json/tc/v1/magazine?page={0}&_embed=true&_envelope=true&categories={1}"

    def __init__(self):
        self.topics = [Topic("Apps", '449557102'), Topic("Startups", "20429"), Topic("Gadgets", "449557086")]
        self.connector = Connection()
        self.max_fails = 5

    @staticmethod
    def __get_link_from_response(response: List[dict]):
        links = []
        for item in response:
            links.append(item['link'])
        return links

    def walk(self, source):
        conn = self.connector.get_connection(source)
        if not conn:
            return None

        json_response = json.loads(conn.read().decode("utf-8"))

        links = self.__get_link_from_response(json_response['body'])
        for link in links:
            r = self.connector.get_connection(link)
            parser = PageParser(["og:title", "og:description"])
            if not r:
                return None

            tmp = parser.get_og_tags(r)
            if tmp:
                tmp["link"] = link
                yield tmp

    def content_loop(self, topic):
        current_page = 1
        saver = Saver(topic.title, ["topic", "link", "og:title", "og:description"])
        fails = 0
        while fails < self.max_fails:
            source = self.topic_pattern.format(str(current_page), topic.code)
            links = self.walk(source)
            
            if not links:
                fails += 1
                continue

            for tags in links:
                tags["topic"] = topic.title
                saver.save(tags)

            current_page += 1

    def get_content(self):
        with ThreadPoolExecutor(max_workers=min(4, len(self.topics))) as executor:
            executor.map(self.content_loop, self.topics)


if __name__ == "__main__":
    cr = Crawler()
    cr.get_content()
