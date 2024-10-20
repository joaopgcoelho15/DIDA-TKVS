#!/bin/bash

# Build the project
mvn clean install

# Launch 5 server terminals
for i in {0..4}
do
  xterm -e "cd server && mvn exec:java -Dexec.args='8080 $i'; exec bash" &
done

# Launch client terminal
xterm -e "cd client && mvn exec:java -Dexec.args='1 -i'; exec bash" &
xterm -e "cd client && mvn exec:java -Dexec.args='2 -i'; exec bash" &

# Launch consoleClient terminal
xterm -e "cd consoleClient && mvn exec:java; exec bash" &