"""
 To use its formater within  the  normal python logger please add the
following snippets to your main code:

from xf_basepy.jsonlog import CustomisedJSONFormatter
import logging

# create logger
logger = logging.getLogger('DeleteFeed')
logger.setLevel(logging.INFO)

# create console handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomisedJSONFormatter())
logger.addHandler(ch)

#To log metric counter
logger.info('metrics', extra=dict(usage_counter))

#For normal log messages
logger.error('foo bar')
"""
import json_log_formatter
import datetime


class CustomisedJSONFormatter(json_log_formatter.JSONFormatter):
    def json_record(self, message, extra, record):
        if message == "metrics":
            extra['name'] = record.name
            extra['timestamp'] = "{:%Y-%m-%dT%H:%M:%SZ}".format(
                datetime.datetime.utcnow())
            extra['level'] = record.levelname
        else:
            extra['name'] = record.name
            extra['message'] = message
            extra['timestamp'] = "{:%Y-%m-%dT%H:%M:%SZ}".format(
                datetime.datetime.utcnow())
            extra['level'] = record.levelname
        return extra
