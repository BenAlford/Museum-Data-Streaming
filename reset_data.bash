#!/bin/bash
source .env
psql -U $DB_USERNAME -d $DB_NAME -h $DB_HOST -c "TRUNCATE TABLE exhibition_request, exhibition_rating RESTART IDENTITY;"