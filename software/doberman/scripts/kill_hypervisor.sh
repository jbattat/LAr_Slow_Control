#!/bin/bash

for screen_name in "pl_convert" "pl_control" "pl_alarm" "hypervisor"; do
    if [[ -n $(screen -ls | grep $screen_name ) ]]; then
	echo "Killing existing screen $screen_name"
	screen -S $screen_name -X quit
    fi
done
