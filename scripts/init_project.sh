#!/bin/bash

echo "
     _    _       __ _               
    / \  (_)_ __ / _| | _____      __
   / _ \ | | '__| |_| |/ _ \ \ /\ / /
  / ___ \| | |  |  _| | (_) \ V  V / 
 /_/   \_\_|_|  |_| |_|\___/ \_/\_/                                                          
"

# Name of the network
NETWORK_NAME=$1

# Name of the Airflow logs
DIR="./airflow/logs"

# Check if the network exists
if [ $(docker network ls --filter name=^${NETWORK_NAME}$ --format="{{ .Name }}" | wc -l) -eq 0 ]; then
  # If the network does not exist, create it
  docker network create -d bridge ${NETWORK_NAME}
else
  echo "The ${NETWORK_NAME} network already exists."
fi

# Check if the directory exists
if [ -d "$DIR" ]; then
  # Get the permissions of the directory
  PERM=$(stat -c '%a' "$DIR")

  # Check if the permissions are 777
  if [ "$PERM" -ne "777" ]; then
    # If not, change the permissions to 777
    chmod 777 -R "$DIR"
    echo "Permissions for $DIR have been changed to 777."
  else
    echo "$DIR already has 777 permissions."
  fi
else
  echo "$DIR does not exist."
fi


