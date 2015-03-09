import mwclient
import mwparserfromhell as mwparser
import sys

sys.path.append("../dfs/")

import client as dfs

site = mwclient.Site('en.wikipedia.org')
category = site.Pages['Category:Big_data']
counter = 0

with dfs.file_appender('/wikipedia/__toc__') as toc:
    for page in category:
        page_filename = "/wikipedia/page%d" % counter
        with dfs.file_appender(page_filename) as f:
            f.write(mwparser.parse(page.text()).strip_code().encode('utf-8'))
        toc.write("%s %s" % (page_filename, page.name))
        counter += 1

with dfs.file_appender('/wikipedia/__size__') as size:
    size.write(str(counter))