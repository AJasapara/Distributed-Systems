#!/bin/bash                                                                     

for i in {1..10}
do
    go test > "output$i" &
done
wait