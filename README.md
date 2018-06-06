# some instructions

## Detecting keywords

Edit [keywords.txt](keywords.txt) to add keywords relevant to what you want to find -- one word per line.


Run [detect_keywords.py](detect_keywords.py), which will iterate through all of `datadump/patreon` and look for the keywords specified in [keywords.txt](keywords.txt)

```sh
$ python detect_keywords.py
```


### The results

The result of the script will be to generate a new version of the [mydata/2015-04.csv](mydata/2015-04.csv), with a few extra columns. The result files can be found at:

- Full version (with all entries, with and w/o keywords: [mydata/detected-2015-04.csv](mydata/detected-2015-04.csv)
- Slim version (just entries with keywords): [mydata/filtered-detected-2015-04.csv](mydata/filtered-detected-2015-04.csv)

This results file omits the invalid entries in the original data file (where `Graphtreon` had a pattern like '?user='), and a few new columns:

    slug, patreon_url, keywords_count, keywords_found

Filter the results file by `keywords_count > 0` to find all records that had matching keywords.




### fetch.py

Running [fetch.py](fetch.py) will read [mydata/2015-03.csv](mydata/2015-03.csv) and download pages into [datadump/patreon](datadump/patreon), but at this point (2018-06-01), [datadump/patreon](datadump/patreon) should contain all of the relevant pages (~100K).
