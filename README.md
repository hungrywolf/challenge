#### Data Engineering - Coding Challenge

##### Project Configuration

This directory contains basic, SBT managed project stub for the Spark application that should be developed for the challenge.
The project may be further developed using Scala or Java and any Spark version and **must** contain at least one (unit) test to demonstrate execution of the application. 
It is allowed to add any dependencies required.

##### Test Data

Test input data files are located in `src/test/resources/data/` and contain information about Anime movies and their ratings, headers are available in the data files. 

##### Assignment

Write a Spark application that determines the top 10 most rated TV series' **genres** of TV series with over 10 episodes.
Prepare a properly configures SBT project with a unit test that executes the application logic using the `sbt test` command and prints statistics on the screen or saves them in the text file.

