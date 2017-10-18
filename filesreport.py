import json
import os
import re
import sqlite3
import csv
from collections import defaultdict

key_re = re.compile(r'files\.consumerfinance\.gov(\/.+$)')

conn = sqlite3.connect('crawl.db')

c = conn.cursor()
query = "select * from links where ultimate_destination like" \
        "'%files.consumerfinance.gov/%'"

results = defaultdict(list)


def asset_filter(key):
    if 'credit-card-agreements' in key:
        return False
    if 'hud/pdfs' in key:
        return False

    _, ext = os.path.splitext(key)
    interesting_types = ['.pdf',
                         '.doc',
                         '.docx',
                         '.xls',
                         '.xlsx',
                         '.csv',
                         '.mov',
                         '.mp3',
                         '.mp4',
                         '.pptm',
                         '.pptx',
#                         '.jpg',
#                         '.png',
                         '.txt',]
    if ext.lower() in interesting_types:
        return True

    return False


for source, destination, ultimate_status, ultimate_destination in c.execute(query):
    match = key_re.search(ultimate_destination)
    key = match.groups()[0]
    if asset_filter(key):
        results[key].append(source)

with open('s3_assets.json') as s3_assets_file:
    all_assets = json.load(s3_assets_file)

for asset in all_assets['Contents']:
    if asset['Key'].startswith('/'):
        key = asset['Key']
    else:
        key = '/' + asset['Key']
    if not asset_filter(key):
        continue
    if key not in results:
        results[key] = []

alphabetically = sorted(results.iteritems(), key=lambda x: x[0])
by_links = sorted(alphabetically, reverse=True, key=lambda x: len(x[1]))
with open('asset_links.csv', 'w') as link_count_file:
    out = csv.writer(link_count_file)
    keys = sorted(results.keys())
    for key, links in by_links:
        row = 0
        label = key
        if len(links) == 0:
            out.writerow([label,'potential orphan' ])
        for link in links:
            out.writerow([label,link ])
            row +=1
            label = None

