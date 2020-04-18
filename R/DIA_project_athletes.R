#-----------------------------------------------------------------------
#                 DIA project - Apr 2020
#                    Philippe Tap 
#               DIA_project_athletes.R
#-----------------------------------------------------------------------
# This script deals with the ETL of the athlete_events.csv Olympic athletes file

# Load required libraries 
library(stringr)
library(Amelia)
library(dplyr)
library(data.table)

########################################################################
#                  Read the dataset
########################################################################

# The script assumes that the athlete_events.csv file is in the same directory

athleteData <- read.csv("athlete_events.csv", header=T, na.strings=c(""), stringsAsFactors = F)

# Summary of the data
summary(athleteData)
str(athleteData)
athleteData

names(athleteData)

####################################################################
# Restrict data to 
# - Summer Olympics
# - meaningful columns
####################################################################

# For DIA Project we will take all the data since 1896

athleteSummerData = athleteData [athleteData$Season == 'Summer',c("ID", "Name" ,"Sex", "Team","NOC","Year","Sport","Event","Medal" )]   
summary(athleteSummerData)
str(athleteSummerData)

# Rename NOC column to Country
names(athleteSummerData)[names(athleteSummerData) == "NOC"] <- "Country"
athleteSummerData

names(athleteSummerData)


##################################################################
#  Convert Medal and Sex to factors
##################################################################
athleteSummerData$Medal  = as.factor (athleteSummerData$Medal)
athleteSummerData$Medal <- ordered(athleteSummerData$Medal , levels = c("NA","Bronze", "Silver", "Gold"))

athleteSummerData$Sex  = as.factor (athleteSummerData$Sex)

str(athleteSummerData)

########################################################################
#                  DATA CLEANUP 
########################################################################

###################################################################
# First, investigate any NA values 
##################################################################

# No NA values per column
sapply(athleteSummerData,function(x) sum(is.na(x)))

# Double check this - Medal = NA for athletes without medal
athleteSummerData[is.na(athleteSummerData$Medal), ]

# Missing values vs observed 
missmap(athleteSummerData, main = "Missing values vs observed")

#
# IMPORTANT : The medal data is not NA is R but "NA" 
# So is.na functions , see above, do not return data 

# No 'NA' in most columns
sum( athleteSummerData$Name == 'NA')
sum (athleteSummerData$Sex == 'NA')
sum( athleteSummerData$Team == 'NA')
sum( athleteSummerData$Country == 'NA')
sum (athleteSummerData$Year == 'NA')
sum (athleteSummerData$Sport == 'NA')
sum( athleteSummerData$Event == 'NA')

# As expected, 'NA' is a value for medal, meaning the athlete did not get a medal 
# No action needed
sum (athleteSummerData$Medal == 'NA')


###################################################################
# IMPORTANT : Ensure no ',' inside the columns
##################################################################

grep (',' , athleteSummerData$Name )

# Find ',' in each of the columns
subset(athleteSummerData , grepl(",", athleteSummerData$Name) ) 
subset(athleteSummerData , grepl(",", athleteSummerData$Sex) )
subset(athleteSummerData , grepl(",", athleteSummerData$Team) ) 
subset(athleteSummerData , grepl(",", athleteSummerData$Country) )
subset(athleteSummerData , grepl(",", athleteSummerData$Year) )
subset(athleteSummerData , grepl(",", athleteSummerData$Sport) )
subset(athleteSummerData , grepl(",", athleteSummerData$Event) ) 
subset(athleteSummerData , grepl(",", athleteSummerData$Medal) )

# Remove all ',' in Name, Team, Event, 
athleteSummerData$Name<- str_remove_all(athleteSummerData$Name, ",")
athleteSummerData$Team<- str_remove_all(athleteSummerData$Team, ",")
athleteSummerData$Event<- str_remove_all(athleteSummerData$Event, ",")

###################################################################
# Order and Save the data frame 
###################################################################

# Order the data frame

athleteSummerData = athleteSummerData[with (athleteSummerData, order (Year,Sport,Event,Medal,Country) ),]

athleteSummerData


########################################################################################
#   DATA TRANSFORMATION 
########################################################################################

# Transform data frame in a table to allow for aggregation

athleteTable <- as.data.table(athleteSummerData)


# In order to count the unique entries and medals for a country, we need 
# to determine if an athlete's entry in individual or part of a team entry
# Lets's mark each row as Indvidual or Team based on the event 

# Initialise all rows as Individual, we will overwrite this in case of a 
# Team event 
athleteTable$EventType <- "Individual"

# The following classification was established after research on Wikipedia
 
athleteTable$EventType [athleteTable$Event %like% "Team" ] =  "Team"
athleteTable$EventType [athleteTable$Event %like% "4 X" ] =  "Team"
athleteTable$EventType [athleteTable$Event %like% "Relay" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Group" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Duet" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Pairs" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Doubles" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Double Sculls" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Two Person" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Three Person" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Fours" ] =  "Team"
athleteTable$EventType [athleteTable$Event %like% "Quadruple" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Eights" ] =  "Team"
athleteTable$EventType [athleteTable$Event %like% "Relay" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Madison" ] = "Team"	
athleteTable$EventType [athleteTable$Event %like% "Synchronized Platform" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Synchronized Springboard" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Multihull" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Baseball" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Basketball" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Football" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Handball" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Hockey" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Volleyball" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Water Polo" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Rugby" ] = "Team"
athleteTable$EventType [athleteTable$Event %like% "Softball" ] = "Team"

# To help with count the unique entries and medals for a country,
# let's add an Entry Name column
#  - for individual events: name of the athlete
#  - for teams : the team name
#         - the country name: France
#         - in case of multiple enties for an event: France 1, France 2

athleteTable$EntryName <- "NA"

athleteTable$EntryName = ifelse(athleteTable$EventType == "Team", athleteTable$Team, athleteTable$Name)

########################################################################
# Save to clean transformed data to a csv file
# Note: file is saved with quotes for now
# The file can be edited manually in Notepad ++ to remove the quotes,
# before it is used.
########################################################################

write.csv(athleteTable,"summer_athletes.csv", row.names = FALSE, quote=FALSE )


