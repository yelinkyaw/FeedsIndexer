#!/usr/bin/env python

INDEX_DIR = "IndexFeeds.index"

import sys, os, lucene, threading, time, re, feedparser

from datetime import datetime
from java.io import File
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import FieldInfo, IndexWriter, IndexWriterConfig
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

class Ticker(object):

    def __init__(self):
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1.0)

class IndexFeeds(object):
    """Usage: python IndexFeeds <feeds_url>"""

    def __init__(self, url, storeDir, analyzer):
        if not os.path.exists(storeDir):
            os.mkdir(storeDir)

        store = SimpleFSDirectory(File(storeDir))
        analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)
        config = IndexWriterConfig(Version.LUCENE_CURRENT, analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(store, config)

        self.indexDocs(url, writer)
        ticker = Ticker()
        print 'commit index',
        threading.Thread(target=ticker.run).start()
        writer.commit()
        writer.close()
        ticker.tick = False
        print 'done'

    def indexDocs(self, url, writer):
        type1 = FieldType()
        type1.setIndexed(True)
        type1.setStored(True)
        type1.setTokenized(False)
        type1.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS)
        
        type2 = FieldType()
        type2.setIndexed(True)
        type2.setStored(True)
        type2.setTokenized(True)
        type2.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
        
        # Read Feeds
        feeds = feedparser.parse(url)

        for item in feeds["entries"]:
            print "adding", item["title"] 
            try:
                link = item["link"] 
                contents = item["description"].encode("utf-8")
                contents = re.sub('<[^<]+?>', '', ''.join(contents))
                title = item["title"]
                doc = Document()
                doc.add(Field("url", link, type1))
                doc.add(Field("title", title, type1))
                if len(contents) > 0:
                    doc.add(Field("contents", contents, type2))
                else:
                    print "warning: no content in %s" % item["title"] 
                writer.addDocument(doc)
            except Exception, e:
                 print "Failed in indexDocs:", e

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print IndexFiles.__doc__
        sys.exit(1)
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print 'lucene', lucene.VERSION
    start = datetime.now()
    try:
        base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        IndexFeeds(sys.argv[1], os.path.join(base_dir, INDEX_DIR),
                   StandardAnalyzer(Version.LUCENE_CURRENT))
        end = datetime.now()
        print end - start
    except Exception, e:
        print "Failed: ", e
        raise e
