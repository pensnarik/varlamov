#!/bin/bash

psql -U postgres -f drop.sql -v database=database
psql -U postgres -f install.sql -v database=database
