#!/bin/bash

set -e

echo "Stopping containers..."

docker compose down -v

echo "Removing old directories..."

sudo rm -rf /home/catpuccino/Desktop/kafka/data/gold_output/users
sudo rm -rf /home/catpuccino/Desktop/kafka/data/output/users
sudo rm -rf /home/catpuccino/Desktop/kafka/data/checkpoint/users/gold
sudo rm -rf /home/catpuccino/Desktop/kafka/data/checkpoint/users/silver

echo "Recreating directories..."

mkdir -p /home/catpuccino/Desktop/kafka/data/gold_output/users
mkdir -p /home/catpuccino/Desktop/kafka/data/output/users
mkdir -p /home/catpuccino/Desktop/kafka/data/checkpoint/users/gold
mkdir -p /home/catpuccino/Desktop/kafka/data/checkpoint/users/silver

sudo chmod -R 777 data

echo "Done."
