import re
import sqlite3
from scrapy.utils.url import strip_url
from urlnorm import norm

conn = sqlite3.connect('crawl.db')

FOLLOW_LINK_PATTERNS = [r'https*:\/\/www.consumerfinance.gov/.*',
                        r'https*:\/\/s3.amazonaws.com\/files.consumerfinance.gov\/.*',
                        r'https*:\/\/files.consumerfinance.gov\/.*',
                        ]
def should_follow(url):
    return any(re.search(p, url) for p in FOLLOW_LINK_PATTERNS)

def follow_link(url, normalized = False):
    url = norm(url)
    query = 'select status, next from results where url=?'
    c = conn.cursor()
    c.execute(query, (url.strip(),))
    try:
        status, next = c.fetchone()
    except TypeError:  # no results
        if '#' in url:
            return follow_link(strip_url(url))

        if not normalized:
            return follow_link(norm(url), normalized=True)
        print "could not find result for %s" % url
        return None, url
    if status in ['301','302']:
        if not should_follow(next):
            print "not following redirect to %s" % next
        return follow_link(next)
    else:
        return status, url
    
links_cursor = conn.cursor()
updates_cursor = conn.cursor()
try:
    links_cursor.execute("alter table links add column ultimate_status")
    links_cursor.execute("alter table links add column ultimate_destination")
    conn.commit()
except sqlite3.OperationalError:
    pass # columns already created

query = "select distinct(destination) from links"
for row in links_cursor.execute(query):
    original_url = row[0]
    if not should_follow(original_url):
        continue
    status, url = follow_link(original_url)
    if not original_url == url:
        print "%s -> %s" % (original_url, url)
        updates_cursor.execute("update links set ultimate_status=?, ultimate_destination=? where destination =?",[status, url, original_url])
        conn.commit()
