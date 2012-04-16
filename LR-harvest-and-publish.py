# OAI-PMH to Learning Registry Publish Utility
# Version 1.0 2011-09-14
# Version 2.0 2012-02-14 
#
# Copyright 2011 US Advanced Distributed Learning Initiative
# c2012 University of Manchester
#
# Change Log
# V 1.0 -- initial public version developed by US Advanced Distributed Learning Initiative
# V 2.0 -- Mimas (NS) customised script to publish Jorum data

import urllib2, json, sys, getpass, StringIO
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from elementtree.ElementTree import Element, parse


username = 'fred'
publish_url = 'http://alpha.mimas.ac.uk/publish'    
oai_url  = 'http://dspace.jorum.ac.uk/oai/request'

class CancelledError(Exception): pass

def convert_to_envelope(doc, rawMetadata, uri, keys):
   #add code here to create the document from the Dublin Core metadata
    doc = {
		"doc_type":           'resource_data',
		"doc_version":        "0.23.0",
		"active":             True,
		"resource_data_type": "metadata",
		"identity":{
			"submitter_type":     "agent",
			"submitter":          "Mimas",
			"curator":            "Jorum",
			"owner":              "Jorum",
		},
		"TOS": {
			"submission_TOS":           "http://www.jorum.ac.uk/terms-of-service"
		},
# -- signature values -- delete this block if not signing
#          "digital_signature": { 			
#              "signature":	"<<signature>>",
#              "key_server":	["<<keyserver>>"],
#              "key_owner":	"<<keyowner>>"
#          },
# -- end of signature values -- end of block to delete if not signing
		"resource_locator":   uri,
		"keys":               keys,
		"payload_placement":  "inline",
		"payload_schema":     ["oai_dc"],
		"resource_data":      rawMetadata,
		"publishing_node":    'local',
    }   
    return doc
			
def acquire_and_publish_documents(oai_url, publish_url, reader, prefix, pwd):
    registry = MetadataRegistry()
    registry.registerReader(prefix, reader)
    client = Client(oai_url, registry)
    documents = []
    count = 0
    for record in client.listRecords(metadataPrefix=prefix):
	    header = record[0]
	    metadata = record[1]
	    rawMetadata = urllib2.urlopen("{0}?verb=GetRecord&metadataPrefix={1}&identifier={2}".format(oai_url,prefix,header.identifier())).read()

            # re-format Jorum id
            identifier = header.identifier()
            identifier = identifier.replace("oai:dspace.jorum.ac.uk:","")
            uri = "http://dspace.jorum.ac.uk/xmlui/handle/" + identifier
            print(uri)

            # create keys from dc.subject terms
            fo = StringIO.StringIO(rawMetadata)
            tree = parse(fo)  # can only parse files or file objects
            keys = []
            for elem in tree.getiterator():
#                print("tag  " + str(elem.tag))
#                print("text " + str(elem.text))
                if elem.tag == "{http://purl.org/dc/elements/1.1/}subject":
                    keys.append(elem.text)
            fo.close()
            print(keys)
            print("\n")
	    value = convert_to_envelope(metadata, rawMetadata, uri, keys)
#            print (value)
#            print(dir(header))
	    if value != None:
		    documents.append(value)
		    count += 1
		    if (count % 10 == 0) or (count == 3):
			publish_documents(publish_url, documents, pwd)
			documents = []
    publish_documents(publish_url, documents, pwd)

def publish_documents(publish_url, documents, pwd):    
    data = {'documents':documents}
    headers = {"Content-Type":"application/json"}
    passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
    passman.add_password(None, publish_url, username, pwd)
    authhandler = urllib2.HTTPBasicAuthHandler(passman)
    opener = urllib2.build_opener(authhandler)
    urllib2.install_opener(opener)
    req = urllib2.Request(publish_url, json.dumps(data), headers)
    with open("output.log","a") as f:
        f.write(urllib2.urlopen(req).read())

	
def main():
    print("\nWelcome to OAI-PMH to Learning Registry Publish Utility\n")
    print("You need to authenticate in order to publish.")
    print("OAI-PMH source: " + oai_url)
    print("Node: " + publish_url)
    print("Username: " + username)
    pwd = getpass.getpass(prompt="Enter password (N.B.: you will *not* see any input as you type): ", stream=None)
    if not pwd:
        raise CancelledError()
    acquire_and_publish_documents(oai_url,publish_url,oai_dc_reader,'oai_dc', pwd)
	
if __name__ == '__main__':
   main()

