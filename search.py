#!/usr/bin/env python

INDEX_DIR = "IndexFeeds.index"

import sys, os, lucene, json

from java.io import File
from cgi import parse_qs, escape
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search.highlight import Highlighter
from org.apache.lucene.search.highlight import InvalidTokenOffsetsException
from org.apache.lucene.search.highlight import QueryScorer
from org.apache.lucene.search.highlight import SimpleHTMLFormatter
from org.apache.lucene.search.highlight import TextFragment
from org.apache.lucene.search.highlight import TokenSources
from org.apache.lucene.util import Version

# Init Lucene
lucene.initVM(vmargs=['-Djava.awt.headless=true'])
analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)

def processQuery(searcher, analyzer, command):
	result = ""
	try:
		query = QueryParser(Version.LUCENE_CURRENT, "contents",analyzer).parse(command)
		topDocs = searcher.search(query, 50)
		scoreDocs = topDocs.scoreDocs
		htmlFormatter = SimpleHTMLFormatter()
		highlighter = Highlighter(htmlFormatter, QueryScorer(query))

		for scoreDoc in scoreDocs:
			doc = searcher.doc(scoreDoc.doc)
			contents = doc.get("contents")
			tokenStream = TokenSources.getAnyTokenStream(searcher.getIndexReader(), scoreDoc.doc, "contents", analyzer)
			frags = highlighter.getBestTextFragments(tokenStream, contents, False, 2);
			contents_highlight = ""

			for frag in frags:
				if frag != None and frag.getScore()>0:
					contents_highlight = contents_highlight + frag.toString().replace('"', '\\"')
						
			result = result + '{"Score":"%s", "Url": %s, "Title": %s, "Contents": %s},' % (scoreDoc.score, json.dumps(doc.get("url")), json.dumps(doc.get("title")), json.dumps(contents_highlight))
		if len(result)>0:
			result = "[" + result[:-1] + "]"
		else:
			result = "[]"
	except Exception, e:
		result = '[]'
        return result.encode("utf-8")

def search(environ, start_response):
	# Parse Get Parameters
	raw = parse_qs(environ['QUERY_STRING'])
	query = raw.get('query', [''])[0]

        base_dir = os.path.dirname(os.path.abspath(__file__))
        directory = SimpleFSDirectory(File(os.path.join(base_dir, INDEX_DIR)))
        searcher = IndexSearcher(DirectoryReader.open(directory))
	
	try:
		if len(query)==0:
			data = "Incorrect Parameters"
                        start_response("400 Bad Request", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(data)))])
		else:
			# Process Query
			data = processQuery(searcher, analyzer, query)
                        start_response("200 OK", [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(data)))])
	except Exception, e:
                data = '[]'
                start_response("200 OK", [("Content-Type", "application/json; charset=utf-8"), ("Content-Length", str(len(data)))])

	return iter([data])

if __name__ == '__main__':
	from wsgiref.simple_server import make_server
	srv = make_server('localhost', 8080, search)
	srv.serve_forever()
