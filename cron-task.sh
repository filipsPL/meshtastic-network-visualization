#!/bin/bash

# create the data dir if it doesn't exist
mkdir -p data/

# create the data dir if it doesn't exist
mkdir -p /var/www/meshtastic-network-visualization/data/

# cd to dir when the scripts live
cd /home/user/meshtastic-network-visualization/

# dump the sqlite db to json
python sqlite2json.py

# copy the json to the webserver
cp data/cytoscape_*.json /var/www/meshtastic-network-visualization/data/

# update the cytoscape data file
LC_TIME=C date > /var/www/rzeczy/cyto-mesh/data/cytoscape_data.txt

exit 0