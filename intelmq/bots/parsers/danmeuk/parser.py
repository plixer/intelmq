# -*- coding: utf-8 -*-
"""
Parses simple newline separated list of IPs.

Docs:
"""
import re

import dateutil

from intelmq.lib import utils
from intelmq.lib.bot import ParserBot

FEEDS = {
    'https://dan.me.uk/torlist/?exit': {
        'format': [
            'source.ip'
        ],
    },
    'https://dan.me.uk/torlist/': {
        'format': [
            'source.ip'
        ]
    }
}


class DanMeUkTorParserBot(ParserBot):
    __last_generated_date = None
    __is_comment_line_regex = re.compile(r'^#+.*')
    __date_regex = re.compile(r'[0-9]{4}.[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{2}( UTC)?')

    def parse(self, report: dict):
        feed = report['feed.url']

        raw_lines = utils.base64_decode(report.get("raw")).splitlines()
        comments = list(r for r in raw_lines if self.__is_comment_line_regex.search(r))

        for line in comments:
            if 'Last updated' in line:
                self.__last_generated_date = dateutil.parser.parse(self.__date_regex.search(line).group(0)).isoformat()

        lines = (l for l in raw_lines if not self.__is_comment_line_regex.search(l))
        for line in lines:
            yield line.strip()

    def parse_line(self, line, report):
        event = self.new_event(report)
        self.__process_defaults(event, line, report['feed.url'])
        self.__process_fields(event, line, report['feed.url'])
        yield event

    def __process_defaults(self, event, line, feed_url):
        defaults = {
            ('raw', line),
            ('classification.type', 'tor'),
        }

        for i in defaults:
            if i[0] not in FEEDS[feed_url]['format']:
                if i[1] is None:
                    continue
                else:
                    event.add(i[0], i[1], overwrite=True)

    @staticmethod
    def __process_fields(event, line, feed_url):
        for field, value in zip(FEEDS[feed_url]['format'], line.split(',')):
            if field == 'time.source':
                ts = dateutil.parser.parse(value + ' UTC').isoformat() if not value.endswith(' UTC') else value
                event.add(field, ts)
            else:
                event.add(field, value)

    def recover_line(self, line):
        return '\n'.join(self.tempdata + [line])


BOT = DanMeUkTorParserBot
