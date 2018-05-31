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
from time import sleep

# http://skipperkongen.dk/2016/09/09/easy-parallel-http-requests-with-python-and-asyncio/
import asyncio
from concurrent.futures import ThreadPoolExecutor


BATCH_SIZE = 50
WORKER_COUNT = 5
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
        dest_name.parent.mkdir(exist_ok=True)
        with open(dest_name, 'w') as w:
            scripttext = extract(resp.text)
            w.write(scripttext)
            return {'success': True, 'content': scripttext, 'url': url, 'dest_name': dest_name}
    else:
        msg = "\tNot OK; Status code: {}".format(resp.status_code)
        return {'success': False, 'content': msg, 'url': url, 'dest_name': dest_name}


def make_batches(mylist, num):
    """easy chunking
    https://chrisalbon.com/python/data_wrangling/break_list_into_chunks_of_equal_size/

    Generates list of lists, with each list having num elements
    """
    for i in range(0, len(mylist), num):
        # Create an index range for l of n items:
        yield mylist[i:i+num]



async def batch_fetch(batch):
    """
    From:
    http://skipperkongen.dk/2016/09/09/easy-parallel-http-requests-with-python-and-asyncio/

    batch is a list of tuples:
    [(slug, patreonurl, local_filename), (slug, patreon_url, local_filename)]

    """
    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                fetch_and_extract,
                url,
                dest_name,
            )
            for slug, url, dest_name in batch
        ]

        for d in await asyncio.gather(*futures):
            if d['success']:
                print("Downloaded:", d['url'])
                print("\tWrote:", len(d['content']), 'to:', d['dest_name'])
            else:
                print("Error with:", d['url'])
                print("\t", d['content'])

def main():
    all_paths = gather_paths()
    print("total paths:", len(all_paths))
    paths = [(s, url, dest) for s, url, dest in all_paths if not dest.exists()]
    print("remaining paths:", len(paths))
    print("-------------------------\n\n")
    batches = list(make_batches(paths, BATCH_SIZE))


    for i, batch in enumerate(batches):
        print("\n\n")
        print("Batch {} of {}".format(i, len(batches)))
        print("---------------")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(batch_fetch(batch))
        sleep(1)



if __name__ == '__main__':
    main()


