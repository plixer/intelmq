# -*- coding: utf-8 -*-
"""
Parses simple newline separated list of IPs.

Docs:
"""
import re
import os

import dateutil

from intelmq.lib import utils
from intelmq.lib.bot import ParserBot

class PlixerDomainParserBot(ParserBot):
    __is_comment_line_regex = re.compile(r'^#+.*')

    def parse(self, report: dict):
        feed = os.path.splitext(report['feed.url'])[1]
        self.taxonomy, self.type = self.get_taxonomy(feed)
        raw_lines = utils.base64_decode(report.get("raw")).splitlines()
        comments = list(r for r in raw_lines if self.__is_comment_line_regex.search(r))

        lines = (l for l in raw_lines if not self.__is_comment_line_regex.search(l))
        for line in lines:
            yield line.strip()

    def parse_line(self, line, report):
        if line.endswith('/'):
            line = line[:-1]
        event = self.new_event(report)
        if self.taxonomy == 'extra':
            event.add('extra.classification_type', self.type)
        else:
            event.add('classification.type', self.type)
            event.add('classification.taxonomy', self.taxonomy)
        event.add('source.fqdn', line) 
        yield event

    def get_taxonomy(self, extension):
        if extension == '.43':
           return ('extra', 'apt1')
        elif extension == '.46':
           return ('extra', 'ipcheck')
        if extension == '.48':
           return ('malicious code', 'c2server')
        elif extension == '.49':
           return ('fraud', 'phishing')
        elif extension == '.50':
           return ('malicious code', 'malware')
        elif extension == '.51':
           return ('malicious code', 'ransomware')
        else:
           self.logger.warn('Unknown extension %s.' % extension)

BOT = PlixerDomainParserBot
