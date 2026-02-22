#!/bin/bash
# =====================================================
# Script: cairo_success_customers.sh
# Purpose: Extract unique customer names who live in Cairo
#          and have at least one successful transaction.
# Usage: Run from telecom-data-platform/raw/ folder
# Why: Demonstrates terminal-level ETL/joining skills
#      using awk and pipes, foundational for Data Engineering.
# =====================================================
cd ..
cd raw


awk -F',' '
# Step 1: Read customers.csv (first file)
NR==FNR {
    # If city is Cairo, store customer name in array keyed by customer_id
    if($3=="Cairo") names[$1]=$2
    next
}
# Step 2: Read transactions.csv (second file)
# If customer_id is in Cairo array and transaction status is Success
($2 in names) && ($4=="Success") {
    print names[$2]  # Output customer name
}
' customers.csv transactions.csv | sort | uniq  # Deduplicate names
