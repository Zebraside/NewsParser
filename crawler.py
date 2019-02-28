from og_parser import PageParser
from connection import Connection
from data_saver import CsvSaver

import json
import logging

from typing import List
from concurrent.futures import ThreadPoolExecutor


class Topic:
    def __init__(self, title: str, code: str):
        self.title = title
        self.code = code


class Crawler:
    topic_pattern = "https://techcrunch.com/wp-json/tc/v1/magazine?page={0}&_embed=true&_envelope=true&categories={1}"

    def __init__(self, logfile_name: str = None):
        self.topics = [Topic("Apps", '449557102'),
                       Topic("Startups", "20429"),
                       Topic("Gadgets", "449557086"),
                       Topic("Social", "3457"),
                       Topic("Mobile", "449557028"),
                       Topic("Enterprise", "449557044")]
        self.connector = Connection()
        self.max_fails = 5
        self.num_threads = 6

        if logfile_name:
            logging.basicConfig(filename='{0}.log'.format(logfile_name), filemode='w')

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

        json_response = json.loads(conn.read().decode("utf-8"))  # TODO: investigate if there could be any problems
        # with encoding

        links = self.__get_link_from_response(json_response['body'])
        for link in links:
            r = self.connector.get_connection(link)
            parser = PageParser(["og:title", "og:description"])
            if not r:
                return None

            tags = parser.get_og_tags(r)
            if not tags:
                logging.warning("Couldn't extract all tags. Link: " + link)

            tags["link"] = link
            yield tags

    def content_loop(self, topic):
        current_page = 1
        saver = CsvSaver(topic.title, ["topic", "link", "og:title", "og:description"])
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

    def __split_task(self, workers: int, task, data):
        with ThreadPoolExecutor(self.num_threads) as executor:
            executor.map(self.content_loop, self.topics)

    def run(self):
        self.__split_task(min(self.num_threads, len(self.topics)), self.connector, self.topics)