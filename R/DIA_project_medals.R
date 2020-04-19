#-----------------------------------------------------------------------
#                 DIA project Apr 2020
#                    Philippe Tap 
#                DIA_project_athletes.R
#-----------------------------------------------------------------------
# This script deals with the ETL of the summer.csv Olympic medal file

# Load required libraries 
library(stringr)
library(Amelia)
library(dplyr)
library (data.table)

########################################################################
#                  Read the dataset
########################################################################

# The script assumes that the athlete_events.csv file is in the same directory

medalData <- read.csv("summer.csv", header=T, na.strings=c(""), stringsAsFactors = T)

# Summary of the data
summary(medalData)
str(medalData)
names(medalData)

########################################################################
#       Convert variables 
########################################################################

# Convert Factors to character variables
medalData$Athlete = as.character(medalData$Athlete)
medalData$Discipline = as.character(medalData$Discipline)
medalData$Event = as.character(medalData$Event)
medalData$Sport = as.character(medalData$Sport)
medalData$City = as.character(medalData$City)
medalData$Country = as.character(medalData$Country)

# Order the levels for Medal
print(levels(medalData$Medal))
medalData$Medal <- ordered(medalData$Medal , levels = c("Bronze", "Silver", "Gold"))
print(levels(medalData$Medal))


########################################################################
#                  DATA CLEANUP (missing values)
########################################################################


# Observe missing values - this command takes some time to run
missmap(medalData, main = "Missing values vs observed")

# NA values per column
sapply(medalData,function(x) sum(is.na(x)))

# Check the 4 rows with missing countries
medalData[is.na(medalData$Country), ]

# Year   City         Sport          Discipline         Athlete Country Gender    Event  Medal
# 29604 2012 London     Athletics           Athletics         Pending    <NA>  Women    1500M   Gold
# 31073 2012 London Weightlifting       Weightlifting         Pending    <NA>  Women     63KG   Gold
# 31092 2012 London Weightlifting       Weightlifting         Pending    <NA>    Men     94KG Silver
# 31111 2012 London     Wrestling Wrestling Freestyle KUDUKHOV, Besik    <NA>    Men Wf 60 KG Silver

# Some medals are pending 
medalData [medalData$Athlete == 'Pending', ]

#       Year   City         Sport    Discipline Athlete Country Gender Event  Medal
# 29604 2012 London     Athletics     Athletics Pending    <NA>  Women 1500M   Gold
# 31073 2012 London Weightlifting Weightlifting Pending    <NA>  Women  63KG   Gold
# 31092 2012 London Weightlifting Weightlifting Pending    <NA>    Men  94KG Silver

# Further investigation
# 
# 1) Men Wrestling Freestyle 60 KG - 2012
# On 29 August 2016, a report indicated that a retested sample for silver medalist Besik Kudukhov taken at the time of this event 
# had returned a positive result (later disclosed as dehydrochlormethyltestosterone).
# [2] On 27 October 2016, the IOC stated that they were unaware that Kudukhov had died in a car accident in December 2013 
# at the time the decision to include his samples in the re-analysis process was made. 
# Since such proceedings cannot be conducted against a deceased person, the IOC dropped all disciplinary proceedings 
# against him. As a result, Olympic results that would most likely have been reviewed will remain uncorrected.[3]

medalData[medalData$Athlete=='KUDUKHOV, Besik' & medalData$Gender=='Men' & medalData$Year==2012 & medalData$Medal == 'Silver',]

medalData[medalData$Athlete=='KUDUKHOV, Besik' & medalData$Gender=='Men' & medalData$Year==2012 & medalData$Medal == 'Silver',] ['Country'] <- 'RUS'

medalData [medalData$Athlete == 'Pending', ]
sapply(medalData,function(x) sum(is.na(x)))


# 2) Women 1500M - 2012
# https://en.wikipedia.org/wiki/Athletics_at_the_2012_Summer_Olympics_%E2%80%93_Women%27s_1500_metres
# These developments meant that six of the race's top nine finishers were linked to PED usage. 
# The aforementioned ESPN story called the race "one of the dirtiest in Olympic history."[12]
# In 2017, the IOC officially reassigned the gold medal to Maryam Yusuf Jamal, 
# but pending the outcome of anti-doping proceedings against several lower-placed finishers the silver and bronze remain vacant.
# In 2018, the IOC reallocated silver and bronze medals, upgrading Tomashova despite her doping suspension.[16]

# Year   City         Sport          Discipline         Athlete Country Gender    Event  Medal
# 29604 2012 London     Athletics           Athletics         Pending    <NA>  Women    1500M   Gold

medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012,]
# Year   City     Sport Discipline             Athlete Country Gender Event  Medal
# 29604 2012 London Athletics  Athletics             Pending    <NA>  Women 1500M   Gold
# 29605 2012 London Athletics  Athletics        BULUT, Gamze     TUR  Women 1500M Silver
# 29606 2012 London Athletics  Athletics JAMAL, Maryam Yusuf     BRN  Women 1500M Bronze

# Real results
# 1st place, gold medalist(s)	Maryam Yusuf Jamal	 Bahrain	4:10.74	
# 2nd place, silver medalist(s)	Tatyana Tomashova	 Russia	4:10.90	
# 3rd place, bronze medalist(s)	Abeba Aregawi	 Ethiopia	4:11.03

medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Gold' ,]['Athlete']  <- 'JAMAL, Maryam Yusuf'
medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Gold' ,]['Country'] <- 'BRN'

medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Silver' ,]['Athlete'] <- 'TOMASHOVA, Tatyana'
medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Silver' ,]['Country'] <- 'RUS'

medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Bronze' ,]['Athlete'] <- 'AREGAWI, Abeba'
medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Bronze' ,]['Country'] <- 'ETH'

medalData[medalData$Event=='1500M' & medalData$Gender=='Women' & medalData$Year==2012,]


# 3) Weighlifting Women 63KG - 2012

# Current  data
medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012,]
# Year   City         Sport    Discipline              Athlete Country Gender Event  Medal
# 29933 2012 London Weightlifting Weightlifting              Pending    <NA>  Women  63KG   Gold
# 29934 2012 London Weightlifting Weightlifting TSARUKAEVA, Svetlana     RUS  Women  63KG Silver
# 29935 2012 London Weightlifting Weightlifting    GIRARD, Christine     CAN  Women  63KG Bronze

# Actual results
# 1st place, gold medalist(s)	 Christine Girard (CAN)	A	
# 2nd place, silver medalist(s)	 Milka Maneva (BUL)	A
# 3rd place, bronze medalist(s)	 Luz Acosta (MEX)	A
# NA values per column after cleanup

# Correct the results
medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Gold',]['Athlete'] <- 'GIRARD, Christine'
medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Gold',] ['Country'] <- 'CAN'

medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Silver',]['Athlete'] <- 'MANEVA, Milka'
medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Silver',]['Country'] <- 'BUL'

medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Bronze',]['Athlete'] <- 'ACOSTA, Luz'
medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012 & medalData$Medal == 'Bronze',]['Country'] <- 'MEX'

medalData[medalData$Event=='63KG' & medalData$Gender=='Women' & medalData$Year==2012,]

# 4)  Weighlifting Men  94KG - 2012

# Current 
medalData[medalData$Event=='94KG' & medalData$Gender=='Men' & medalData$Year==2012,]
# Year   City         Sport    Discipline                      Athlete Country Gender Event  Medal
# 29951 2012 London Weightlifting Weightlifting MOHAMMADPOURKARKARAGH, Saeid     IRI    Men  94KG   Gold
# 29952 2012 London Weightlifting Weightlifting                      Pending    <NA>    Men  94KG Silver
# 29953 2012 London Weightlifting Weightlifting                  KIM, Minjae     KOR    Men  94KG Bronze

# Actual results
# 1st place, gold medalist(s)	 Saeid Mohammadpour (IRI)		
# 2nd place, silver medalist(s)	 Kim Min-jae (KOR)	A	
# 3rd place, bronze medalist(s)	 Tomasz Zielinski (POL)	B	

# Correct the results
medalData[medalData$Event=='94KG' & medalData$Gender=='Men' & medalData$Year==2012 & medalData$Medal == 'Silver' ,]['Athlete'] <- 'KIM, Minjae'
medalData[medalData$Event=='94KG' & medalData$Gender=='Men' & medalData$Year==2012 & medalData$Medal == 'Silver' ,]['Country'] <- 'KOR'

medalData[medalData$Event=='94KG' & medalData$Gender=='Men' & medalData$Year==2012 & medalData$Medal == 'Bronze' ,]['Athlete'] <- 'ZIELINSKI,  Tomasz'
medalData[medalData$Event=='94KG' & medalData$Gender=='Men' & medalData$Year==2012 & medalData$Medal == 'Bronze' ,]['Country'] <- 'POL'

# Verify that all NA and Pending values have been fixed
sapply(medalData,function(x) sum(is.na(x)))
medalData[is.na(medalData$Country), ]
medalData [medalData$Athlete == 'Pending', ]

########################################################################
#                  DATA CLEANUP (data anomalies)
########################################################################

# The following entry causes issues with csv, correct the Athlete name
# 1900,Paris,Rugby,Rugby,BINOCHE Jean, Léon,FRA,Men,Rugby,Gold,Rugby Rugby Rugby,Men

medalData [medalData$Athlete == 'BINOCHE Jean, L�f©on', ]

medalData[25036, ]['Athlete'] <- 'BINOCHE Jean'
medalData [medalData$Athlete == 'BINOCHE Jean', ]


########################################################################################
#                   DATA CLEANUP - formatting issues
########################################################################################

# 1. Athlete column: remove the ',' to avoid issues with csv file

grep (',' , medalData$Athlete )

medalData$Athlete <- str_remove_all(medalData$Athlete, ",")


# 2. Event column: remove the ',' to avoid issues with csv file

grep (',' , medalData$Event)

medalData$Event <- str_remove_all(medalData$Event, ",")



########################################################################################
#                   DATA PREPARATION
########################################################################################


# 1. Create a new column for olympic_event as a concatenation of Sport/Discipline/Event
# This is needed to get an Event unique identifier 
medalData$OlympicEvent = paste(medalData$Sport,medalData$Discipline, medalData$Event) 

# 2. Create a column to define the gender of an event : Men, Women and Mixed
# This is required to aggregate medals correctly for a pair of mixed people
# or in case of an event that allows both Men and Women for Individual
# and Teams e.g.  Equestrian

medalData$EventGender =  medalData$Gender
levels(medalData$EventGender) <- c(levels(medalData$EventGender), "Mixed") 


# 3. Populate EventGender with " Mixed" where applicable
##########################################################################
# https://en.wikipedia.org/wiki/Category:Mixed_events_at_the_Olympics
##########################################################################


# 3.1. Mixed doubles tennis /badminton at the Summer Olympics 

############################################################################
# For tennis and  badminton, set the EventGender of Mixed Doubles to Mixed
############################################################################


medalData$EventGender [medalData$Event == 'Mixed Doubles' ] <- 'Mixed'
medalData$EventGender [medalData$Event == 'Mixed Doubles Indoor' ] <- 'Mixed'

medalData [medalData$Event == 'Mixed Doubles', ]


filter ( medalData[c("Sport","Event", "EventGender")] , grepl("Mixed",Event)) 


# 3.2  Equestrian : Mixed for Individual and Teams 

attach (medalData)

medalData [Sport == 'Equestrian',]

# There is no Equestrian event that are Gender specific
filter ( medalData [Sport == 'Equestrian',] , grepl("Woman",Event)) 
filter ( medalData [Sport == 'Equestrian',] , grepl("Men",Event)) 

#[1] Year       City       Sport      Discipline Athlete    Country    Gender     Event      Medal     
#<0 rows> (or 0-length row.names)

##########################################
# Set all Equestrian events to mixed
##########################################

medalData$EventGender [Sport == 'Equestrian' ] <- 'Mixed'

# Review
medalData  [Sport == 'Equestrian' & Year == 2012 , ] 


#############################################################
# 4. Mixed-sex sailing at the Summer Olympics
#############################################################

# All mixed sailing events are indvidual so no action
# In 2016 Nacra17	event was mixed : NA here

#############################################################
# 5. Mixed-sex shooting at the Summer Olympics
#############################################################

# All mixed sailing events are individual so no action

sapply(medalData,function(x) sum(is.na(x)))

sapply(medalData,function(x) sum(is.na(x)))

##################################################################
# This is the final clean version of medal data frame . Save it
##################################################################

write.csv(medalData,"summer_medals.csv", row.names = FALSE, quote=FALSE)

