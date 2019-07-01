#!/bin/bash

function fixnl() {
  echo "Fixing $1..."
  awk '/^$/ {nlstack=nlstack "\n";next;} {printf "%s",nlstack; nlstack=""; print;}' $1 > $1.tmp && mv $1.tmp $1
}

shopt -s globstar

for file in **/*.cpp; do fixnl "$file"; done
for file in **/*.hpp; do fixnl "$file"; done
