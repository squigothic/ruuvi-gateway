#!/bin/bash

FILENAME="loadavg-"$(date '+%Y%m%d')".txt"

uptime >> "$FILENAME"

#echo "fun times"

exit