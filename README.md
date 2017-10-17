# NBA PySpark

The use of distributed computing platforms, like Spark, are becoming more and more valuable in data analysis. To practice using Spark I decided to use it's power to analyze a dataset that I am passionate about: the NBA. NBA data is a great practice tool as the use of data to better understand the NBA has been growing in popularity and is just in its infancy as the amount of data is growing at a fast rate with player tracking data becoming more available. In this analysis I looked at the 2014-2015 NBA Season and used it to implement basic machine learning models and other simple data analysis. The task was more of a practice in using PySpark than ground breaking NBA analysis. I used the University of Michigan's flux server and PySpark 1.6.

## The Data
The input data comes from three seperate files pertaining to the 2014-2015 NBA season. The first is file is the shot log for all shots that were taken during the season, which was found on Kaggle and can be found [here](https://www.kaggle.com/dansbecker/nba-shot-logs/data). The second file is also from Kaggle and can be found [here](https://www.kaggle.com/drgilermo/nba-players-stats-20142015/data) and contains player stats from the season, everything from total minutes and points to height, weight and age. The final dataset was taken from the [NBA's website](https://stats.nba.com/teams/advanced/?sort=W&dir=-1&Season=2014-15&SeasonType=Regular%20Season). I copied the date into a text file and then cleaned it slightly (the code of which can be found in the nba_teams_data.py script). 

## Predicting a shot's outcome
Using information like the distance the defender is from the basket, the distance to the closest defender, touch time befroe shooting the ball, number of dribbles, and more, I wanted to try and predict a shot's outcome. The model that we will use to try and predict a shot's outcome has the distance from the basket, the distance to the closest defender, the shot type and the FG% of that player. The shot type is either a catch and shoot or a shot off the dribble. The FG% for the player is the season total for the 2014-2015 season and is the 3P% if the shot was a 3-Pointer.

Randomly splitting our data into a training (80% of our data) and a test set (20%), we predict how well our logisitic regression model classifies if a shot is made or missed. Unfortunately, the classification accuracy is a low 0.61, showing that it is very difficult to predict if a shot is going to go on in or not as there are many other factors that go into the shot but we can do better than just flipping a coing. Additionally, while shots closer to the basket are generally thought of as easier shots, your defender is also much closer to you which causes a different relationship than when you are at the 3 point line and your defender is the same distance away. Looking at the plot below we start to see the complexity of  predicting the shot's outcome as many made and missed shots are all clustered in the same area. Excuse the blurriness, the transparancy was increased to try and see more of the points.

![](/Data/Output/Plots/shot_outcome.png?raw=true "Shot Outcome based on Defender Distance and Distance to Basket")

Despite our complexities, our model does back up intuitive thinking. In the table below we see the coefficients from the logistic regression that we fit. As one would expect, as you get farther away from the basket (increase your shot distance), the odds that your shot goes in goes down. Additionally, as the defender gets farther away (increasing the defender distance), the odds your shot goes in increases. Nothing ground breaking yet, but it is interesting to see that the coefficient for defender distance is larger in absolute value, showing that your odds of making a shot increases more by moving your defender one foot farther away than you as a shooter moving in one foot, keeping all other factors constant. The Shot tYpe factor is an indicator of whether the shot was a catch and shoot or off the dribble. It is agreed that shooting off the dribble is much hard (just try it yourself) and thus it makes sense that the odds of a shot going in decreases as you go from a catch and shoot to off the dribble. 

<center>
 
| Variable          | Coefficient |
|------------------:|:-----------:|
| Shot Distance     | -0.0553     |
| Defender Distance | 0.1056      |
| Shot Type         | -0.1369     |
| FG%               | 0.0034      |

</center>

## Does scoring get better with experience?
Does it take a rookie a couple years to adapt to the next level? Do older players start to adapt their game by passing more? In our next analysis we looked at the relationship between player's scoring per 48 minutes and how many years they have been in the league. We took all players who played more than 250 minutes in the 2013-2014 season and grouped them by their experience level. In the table and graph below we see the results:

| Experience | n  | Points per 48 min |
|-----------:|:--:|:-----------------:|
| Rookies    |14  | 15.30             |
|1           |26  | 17.81             |
|2           |23  | 19.59             |
|3           |23  | 21.42             |
|4           |22  | 20.41             |
|5           |25  | 21.14             |
|6           |18  | 20.68             |
|7           |16  | 19.47             |
|8           |7   | 23.24             |
|9           |12  | 18.09             |
|10          |11  | 18.36             |
|11          |16  | 19.63             |
|12          |4   | 18.30             |
|13          |6   | 21.07             |
|14+         |9   | 18.96             |

![](/Data/Output/Plots/experience.png?raw=true "Points per 48 Minutes by Experience")

Through this basic analysis our hypothesized thoughts seem to be seen. Scoring seems to increase through the first 3 years in the leagure where it reaches its height between 3 and 6 years of experience. After about 8 years of experience we a decline in scoring for the rest of the years with the 9 years of experience showing the third lowest points per 48 minutes. There is a spike at 13 years experience but due to the small sample size we are weary about the results.

The leaders during the 2013-2014 season were Kevin Durant, LeBron James, Kevin Love and Russell Westbrook who were in their 7th, 11th, 6th and 6th year of experience respectively and were all above 34 points per 48. We point this out because for such small sample sizes, like we have, taking the average within groups can lead to skewed results. This leads to the spike in points per 48 at 8 years of experience. This spike was lead by LaMarcus Aldridge's impressive 30.8 points per 48 and Rudy Gay's 27.6 points per 48.

## What are the physical qualities of NBA "positions"?
It is pretty agreed upon at this point in NBA circles that the 5 original positions that used to define player's positions no longer captures the true positions in the NBA today. With stretch 4's, 3 & D wings, scoring point guards vs traditional pass first point guards, we can no longer pigeonhole players into just a point guard, shooting guard, small forward, power forward and center. As we start to look at clustering players by their qualities a question I have is: What are the physical qualities (height, weight and age) of these new and improved clusters. The motivation behind this analysis came from the general positions. Point guards are smaller and the centers are bigger. Now that we focus more on the individual skills, instead of general buckets, it will be interesting to see how the idea behind player positions change.

The first step was to first define our new "positions", which was done by using k-means clustering to fit players into 8 groups. Since we only have data (for this analysis) from the 2014-2015 season and since we do not have number of possessions, we will use statistics per 48 minutes. Using season totals can be problematic because it would just lead to cluster people based on how often they play. We ended up deciding on clustering by the following statistics:
* Points per 48
* Field Goal Attempts per 48
* FGP%
* 3 Point Attempts per 48
* 3P%
* Free Throw Attempts per 48
* FT%
* Offensive Rebounds per 48
* Deffensive Rebounds per 48
* Asists per 48
* Steals per 48
* Turnoversr per 48

We then filtered out any players that did not play at least 1000 minutes. This was an arbitrary cutoff but felt like enough time for a player to get find an actual role in the game which leaves us with a total of 274 players.

Below we see how our clusters split up. It was pretty difficult to identify the different clusters, not giving us too much confidence in using per 48 minute stats. Saying that, we did see some seperatation and have more confidence in the clusters found than using the general 5 position lineupes.

| Cluster Name        | Num. Players | Example of players                             |
|--------------------:|:------------:|:----------------------------------------------:|
| Scoring Guards      | 10           | LeBron James, Russell Westbrook, Stephen Curry |
| Score first/3 Point Wings | 32     | Chandler Parsons, Danny Green, Kyle Korver     |
| 3 and D Wings       | 32           | Trevor Ariza, Avery Bradly, Gordon Hayward     |
| D first Wings/Stretch Posts | 58   | Andre Iguodala, Iman Shumpert, Patrick Patterson |
| Score First         | 21           | Dwyane Wade, DeMarcus Cousins, DeMar DeRozan   |
| Defensive Bigs      | 43           | DeAndre Jordan, Tristan Thompson, Tyson Chandler |
| Role Players        | 45           | Al-Farouq Aminu, Otto Porter Jr., Matthew Dellavedova |
|  Interesting Mix of Players  | 33  | Carmelo Anthony, Serge Ibaka, Tim Duncan, Tony Parker |

The purpose of this analysis is to see how the physical attributes of these players compare. We will look at how the average age, height and weight compare for the clusters in both tables and figures.
