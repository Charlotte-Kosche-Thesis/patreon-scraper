"""
iterates through graphtreon file to find URLs

downloads and extracts relevant <script> content
and saves to datadump/patreon/overviews/[first_letter_of_slug]/slug.html

skips existing files; should only have to run once
"""

from bs4 import BeautifulSoup
import csv
import requests
from pathlib import Path

SRCPATH = Path('datadump', 'graphtreon', '2018-04.csv')
DATA_DIR = Path('datadump', 'patreon', 'overviews')
DATA_DIR.mkdir(parents=True, exist_ok=True)

METATAG = 'Object.assign(window.patreon.bootstrap, {'


def extract(rawhtml):
    """
    too expensive to save entire HTML, so we save just the relevant script tags
    """
    soup = BeautifulSoup(rawhtml, 'lxml')
    scripttags = [str(s) for s in soup.select('script') if METATAG in s.text]
    return '\n'.join(scripttags)


def main():
    records = list(csv.DictReader(SRCPATH.read_text().splitlines()))
    # filter out invalid users
    records = [r for r in records if 'user?u=' not in r['Graphtreon']]
    for i, row in enumerate(records):
        g = row['Graphtreon']
        slug = g.split('/')[-1]
        dest_name = DATA_DIR.joinpath(slug[0].lower(), slug + '.html')
        dest_name.parent.mkdir(exist_ok=True)
        if not dest_name.exists():
            url = "https://www.patreon.com/{}/overview".format(slug)
            print("{} out of {}:".format(i, len(records)), "Downloading", url)
            resp = requests.get(url, allow_redirects=False)
            if resp.status_code == 200:
                with open(dest_name, 'w') as w:
                    scripttext = extract(resp.text)
                    w.write(scripttext)
                    print("\tWrote", len(scripttext), "chars")
            else:
                print("\t", "Not OK; Status code:", resp.status_code)

if __name__ == '__main__':
    main()


