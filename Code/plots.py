# -*- coding: utf-8 -*-
"""
Created on Sun Oct  1 09:36:36 2017

@author: spoonertaylor
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
                                             "12","13","14+"])


#############
# Task 3: Looking at personal types of clusters
############

types = pd.read_csv(path+'output_personal_info.csv')

types_long = pd.melt(types, id_vars = ['Cluster'], value_vars = ['Height', 'Weight', 'Age'])

types_plt = ggplot(aes(x='Cluster', y = 'Age'), data=types) + geom_bar(stat="identity") + facet_wrap("variable") 
 
plt.figure(1)

age = types[['Age']].plot(kind = "bar", title = "Age", legend=False)
age.set_ylabel('Age')
age.set_xlabel('Cluster')

height = types[['Height']].plot(kind = "bar", title = "Height", legend=False)
height.set_ylabel('Height (cm)')
height.set_xlabel('Cluster')

weight = types[['Weight']].plot(kind = "bar", title = "Weight", legend=False)
weight.set_ylabel('Weight (kg)')
weight.set_xlabel('Cluster')


############
# Task 4: Offensive Rating and Proportions
###########
off = pd.read_csv(path+'output_team_props.csv')

teams = ggplot(aes(x='OFF_RAT', y='Prop'),data=off)