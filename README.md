# NBA PySpark

The use of distributed computing platforms, like Spark, are becoming more and more valuable in data analysis. To practice using Spark I will look at the 2014-2015 NBA Season to implement basic machine learning models and other simple data analysis. NBA data is a great practice tool as basketball analytics is continuing to grow in popularity. 

Using the University of Michigan's flux server, I will implement the following analysis in PySpark 1.6. 

The input data comes from three seperate files pertaining to the 2014-2015 NBA season. The first is file is the shot log for all shots that were taken during the season, which was found on Kaggle and can be found [here](https://www.kaggle.com/dansbecker/nba-shot-logs/data). The second file is also from Kaggle and can be found [here](https://www.kaggle.com/drgilermo/nba-players-stats-20142015/data) and contains player stats from the season, everything from total minutes and points to height, weight and age. The final dataset was taken from the [NBA's website](https://stats.nba.com/teams/advanced/?sort=W&dir=-1&Season=2014-15&SeasonType=Regular%20Season). I copied the date into a text file and then cleaned it slightly (the code of which can be found in the nba_teams_data.py script).
