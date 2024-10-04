#!/bin/bash

# Build the project
mvn clean install

# Launch 5 server terminals
for i in {0..4}
do
  gnome-terminal -- bash -c "cd server && mvn exec:java -Dexec.args='8080 $i'; exec bash"
done

# Launch client terminal
gnome-terminal -- bash -c "cd client && mvn exec:java; exec bash"

# Launch consoleClient terminal
gnome-terminal -- bash -c "cd consoleClient && mvn exec:java; exec bash"