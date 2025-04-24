#!/bin/bash

folder="/home/doberman/doberview2"
screen_name="doberview"

if [[ -n $(screen -ls | grep $screen_name ) ]]; then
  echo "Killing existing screen"
  screen -S $screen_name -X quit
fi

command="cd $folder && node app.js"
echo "$command"
screen -S $screen_name -dm /bin/bash -c "$command"
