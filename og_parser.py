import atexit
from typing import List

from lxml.html import parse
from lxml import etree

from urllib3.response import HTTPResponse


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
        if not self.__parse_tags(r):
            return None
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
