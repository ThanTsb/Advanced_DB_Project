#merge Crime Data 2010-2019 and 2020-Present in one .csv file

#get headers
headers=$(head -n 1 ~/datasets/Crime_Data_from_2010_to_2019.csv)

#get records from both files
records_2010_2019=$(tail -n+2 ~/datasets/Crime_Data_from_2010_to_2019.csv)
records_2020_pres=$(tail -n+2 ~/datasets/Crime_Data_from_2020_to_Present.csv)

#create new .csv file
echo "$headers" > ~/datasets/Crime_Data_from_2010_to_Present.csv
echo "$records_2010_2019" >> ~/datasets/Crime_Data_from_2010_to_Present.csv
echo "$records_2020_pres" >> ~/datasets/Crime_Data_from_2010_to_Present.csv

#also create LAPD_Police_Stations.csv
touch ~/datasets/LAPD_Police_Stations.csv

echo "X,Y,FID,DIVISION,LOCATION,PREC" > ~/datasets/LAPD_Police_Stations.csv
echo "-118.289241553,33.7576608970001,1,HARBOR,2175 JOHN S. GIBSON BLVD.,5" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.275394206,33.9386273800001,2,SOUTHEAST,145 W. 108TH ST.,18" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.277669655,33.9703073800001,3,77TH STREET,7600 S. BROADWAY,12" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.419841576,33.9916553210001,4,PACIFIC,12312 CULVER BLVD.,14" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.305141563,34.0105753400001,5,SOUTHWEST,1546 MARTIN LUTHER KING JR. BLVD.,3" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.256118891,34.012355905,6,NEWTON,3400 S. CENTRAL AVE.,13" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.247294123,34.0440195,7,CENTRAL,251 E. 6TH ST.,1" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.450779541,34.0437774120001,8,WEST LOS ANGELES,1663 BUTLER AVE.,8" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.213067956,34.045008769,9,HOLLENBECK,2111 E. 1ST ST.,4" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.342829525,34.046747682,10,WILSHIRE,4861 VENICE BLVD.,7" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.291175911,34.050208529,11,OLYMPIC,1130 S. VERMONT AVE.,20" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.266979649,34.056690437,12,RAMPART,1401 W. 6TH ST.,2" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.33066931,34.095833225,13,HOLLYWOOD,1358 N. WILCOX AVE.,6" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.249414484,34.119200666,14,NORTHEAST,3353 SAN FERNANDO RD.,11" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.385859348,34.1716939300001,15,NORTH HOLLYWOOD,11640 BURBANK BLVD.,15" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.445225709,34.1837432730001,16,VAN NUYS, 6240 SYLMAR AVE.,9" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.547454438,34.193397227,17,WEST VALLEY,19020 VANOWEN ST.,10" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.599636542,34.221376654,18,TOPANGA,21501 SCHOENBORN ST.,21" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.410417183,34.2530912220001,19,FOOTHILL,12760 OSBORNE ST.,16" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.531373363,34.256969059,20,DEVONSHIRE,10250 ETIWANDA AVE.,17" >> ~/datasets/LAPD_Police_Stations.csv
echo "-118.468197808,34.272979397,21,MISSION,11121 N. SEPULVEDA BLVD.,19" >> ~/datasets/LAPD_Police_Stations.csv