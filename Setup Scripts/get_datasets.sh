#Check if the datasets directory exists
if [ ! -d ~/datasets ]; then
  echo "Creating ~/datasets directory..."
  mkdir ~/datasets
else
  echo "~/datasets directory already exists."
fi

#Get Crime Data from 2010-2019 and 2020-present
wget  -O ~/datasets/Crime_Data_from_2010_to_2019.csv "https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD"
wget  -O ~/datasets/Crime_Data_from_2020_to_Present.csv "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"

#Get additional Datasets
wget -O ~/datasets/data.tar.gz "http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz"

#extract contents
tar -xvf ~/datasets/data.tar.gz -C ~/datasets/

#remove data.tar.gz
rm ~/datasets/data.tar.gz