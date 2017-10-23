

@author: spoonertaylor# -*- coding: utf-8 -*-
"""
Created on Sun Oct  1 09:36:36 2017
"""

from ggplot import *
import ggplot
import pandas as pd
import re
import matplotlib.pyplot as plt
plt.style.use('ggplot')

path = r'C:/Users/Taylor/Documents/UMich/SI_618/Project/Data/Output/'

###########
# Task 1: Shots
###########
rgx = re.compile('[%s]' % '()')
shots = pd.read_csv(path+'output_shots.csv')
# Clean data.
shots['FEATURES'] = shots['FEATURES'].apply(lambda x: rgx.sub('', x))
shots[['DIST', 'DEF_DIST', 'SHOT_TYPE', 'FG%']] = shots.FEATURES.apply(lambda x: pd.Series(x.split(',')))\
     .apply(pd.to_numeric, errors='coerce')
del shots['FEATURES']
# Plot data
shots_plt = ggplot(aes(x = 'DIST', y='DEF_DIST', color = 'SHOT_OUTCOME', shape = 'SHOT_OUTCOME'), data = shots) + geom_point(alpha=.25)

shots_plt.save("shot_outcome.png")
###########
# Task 2: Player's scoring per 48 by experience
###########
exp = pd.read_csv(path+'output_experience.csv')
exp['Experience'] = list(range(0,15))
exp_plt = ggplot(aes(x = 'Experience', y = 'POINTS'), data=exp) + geom_point() + geom_line() + \
                scale_x_continuous(breaks = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14], \
                                   labels = ["0","1","2","3","4","5","6","7","8","9","10","11",\
                                             "12","13","14+"]) + ylab("Points per 48 min")


#############
# Task 3: Looking at personal types of clusters
############

types = pd.read_csv(path+'output_personal_info.csv')
types = types.sort_values('Cluster')
index = ['Defensive Wings', '3 and D Wings', 'Role Players', '3 Point Wings', 'Scoring Guards',\
         'Score First Mindset', 'Interesting Mix', 'Defensive Bigs']
types['Index'] = index
types_long = pd.melt(types, id_vars = ['Cluster', 'Index'], value_vars = ['Height', 'Weight', 'Age'])

types_plt = ggplot(aes(x='Cluster', weight = 'value'), types_long) + geom_bar() + \
                  facet_wrap("variable", scales = "free", nrow=3)
                  
############
# Task 4: Offensive Rating and Proportions
###########
off = pd.read_csv(path+'output_team_props.csv')
off['OFF_RAT'] = pd.Categorical(off['OFF_RAT'], ["[93, 96)", "[96, 99)", "[99, 102)","[102, 105)", "[105, 111)"])
off = off.sort_values('OFF_RAT')
off['index'] = [0,0,0,1,1,1,2,2,2,3,3,3,4,4,4]

teams = ggplot(aes(x='index', y='Prop', group='SHOT_TYPE', color='SHOT_TYPE'),data=off) + geom_point() + geom_line() + \
                scale_x_continuous(breaks = [0,1,2,3,4], \
                                   labels = ["[93, 96)", "[96, 99)", "[99, 102)","[102, 105)", "[105, 111)"]) + \
        xlab("Offensive Rating") + ylab("Proportion of Shots")