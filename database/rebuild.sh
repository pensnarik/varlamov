#!/bin/bash
docker build . -t database && docker-compose rm -f database && docker-compose up database
