#!/bin/bash

for view in 1 2
do
  for t in {2..8}
  do
    output=$(./mandelbrot -t $t -v $view)
    speedup=$(echo "$output" | grep -oP '\(\K[0-9.]+(?=x speedup)')
    echo "view: $view, threads: $t, speedup: $speedup"
  done
done