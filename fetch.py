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
import asyncio
# http://skipperkongen.dk/2016/09/09/easy-parallel-http-requests-with-python-and-asyncio/



SRCPATH = Path('datadump', 'graphtreon', '2018-04.csv')
DATA_DIR = Path('datadump', 'patreon', 'overviews')
DATA_DIR.mkdir(parents=True, exist_ok=True)

METATAG = 'Object.assign(window.patreon.bootstrap, {'


def gather_slugs():
    records = list(csv.DictReader(SRCPATH.read_text().splitlines()))
    # filter out invalid users
    slugs = [r['Graphtreon'].split('/')[-1] for r in records if 'user?u=' not in r['Graphtreon']]
    return slugs

def gather_paths():
    """
    returns a list of tuples:
        [(slug, patreonurl, local_filename), (slug, patreon_url, local_filename)]
    """
    paths = []
    for slug in gather_slugs():
        url = "https://www.patreon.com/{}/overview".format(slug)
        dest_name = DATA_DIR.joinpath(slug[0].lower(), slug + '.html')
        paths.append((slug, url, dest_name))
    return paths


def extract(rawhtml):
    """
    too expensive to save entire HTML, so we save just the relevant script tags
    """
    soup = BeautifulSoup(rawhtml, 'lxml')
    scripttags = [str(s) for s in soup.select('script') if METATAG in s.text]
    return '\n'.join(scripttags)


def fetch_and_extract(url, dest_name):
    resp = requests.get(url, allow_redirects=False)
    if resp.status_code == 200:
        with open(dest_name, 'w') as w:
            scripttext = extract(resp.text)
            w.write(scripttext)
            return {'success': True, 'content': scripttext, 'url': url, 'dest_name': dest_name}
    else:
        msg = "\tNot OK; Status code: {}".format(resp.status_code)
        return {'success': False, 'content': msg, 'url': url, 'dest_name': dest_name}


async def batch_fetch(paths):
    """
    paths is a list of tuples:
    [(slug, patreonurl, local_filename), (slug, patreon_url, local_filename)]

    """
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            executor,
            fetch_and_extract,
            url,
            dest_name,
        )
        for slug, url, dest_name in paths
    ]
    for d in await asyncio.gather(*futures):
        if d['success']:
            print("Downloaded:", d['url'])
            print("\tWrote:", len(d['content']), 'to:', d['dest_name'])
        else:
            print('Error:', d['content'])



#     loop = asyncio.get_event_loop
#
#    futures = [loop.run_in_executor(None, fetch_and_extract, slug)
#                for slug in gather_slugs]


def main():
    all_paths = gather_paths()
    print("total paths:", len(all_paths))
    paths = [(s, url, dest) for s, url, dest in all_paths if not dest.exists()]
    print("remaining paths:", len(paths))
    print("-------------------------\n\n")

    batch = paths[0:20]
    batch_fetch(batch)


if __name__ == '__main__':
    main()


