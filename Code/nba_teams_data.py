# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 22:29:40 2017

@author: spoonertaylor
"""
import os
import pandas as pd
os.chdir("C:/Users/Taylor/Documents/UMich/SI_618/Project/")

# Load in the copied text file
with open(r'NBA_Teams.txt', 'r') as file:
    data = file.readlines()

# Split the data based on the tabs
data = data[1:len(data)]
data2 = [j.split('\t') for j in data]

# Values for the teams
data_vals = data2[1:len(data2)]
# For each team, clean the data
for team in data_vals:
    team[18] = team[18].replace('\n', '')
    for i in range(1,len(team)):
        team[i] = float(team[i])
# Clean column name
cols = data2[0]
cols[18] = 'PIE'
# Turn to data frame
teams = pd.DataFrame(data_vals)
teams.columns = cols
teams = teams.sort_values('TEAM')
teams['TEAM_ABBV'] = ['ATL', 'BKN', 'BOS','CHA','CHI',
                     'CLE', 'DAL','DEN', 'DET','GSW',
                     'HOU', 'IND', 'LAC', 'LAL','MEM',
                     'MIA', 'MIL', 'MIN', 'NOP','NYK',
                     'OKC', 'ORL', 'PHI', 'PHX', 'POR',
                     'SAC', 'SAS', 'TOR','UTA','WAS']
# Write to csv
teams.to_csv("NBA_Teams.csv")