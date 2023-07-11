sqlite3 datacommons.db <<EOF
.headers on
.mode csv
.import observations.csv observations
EOF