## Exercise:
[We have a happiness database which was extracted from the Wallethub.com analysis on the happiest states in America.] (https://wallethub.com/edu/happiest-states/6959/) 

### Methodology
In order to determine the happiest states in America, WalletHub compared the 50 states across three key dimensions: 1) Emotional & Physical Well-Being `Well Being`, 2) Work Environment `Work` and 3) Community & Environment `Environment`. The key dimensions were combined to create a happiest ranking `Rank` for the each state.

The last step in data cleaning is to review the relationships between the response variable and the explanatory variables. In order to calculate correlation coefficients we first need to `cast` the spark dataframe column types to `float`.  

Next, we create a list of the column names to be added to the output list of correlations. The `Rank` variable is our response variable so we select that first, followed by the numeric explanatory variables `Well Being, Work, Environment`.

Once we have our input spark dataframe we can write a for loop to iterate over the rows and columns and calculate the correlation coeficients along the way. Then we append those ['i,j'] correlations to the `core` list.

Using the `core` list we can create a new spark dataframe with the column names its corresponding correlation value.


## Instructions:
###The Happiness spark dataframe has been loaded into the workspace

```python
#### Convert strings to floats by using the `cast` function, first select the numeric variables from the dataframe.    
flts = df.select(df['Rank'].cast("float"),df['Total Score'].cast("float"), df['Well Being'].cast("float),df['____'].___("float"),df.['____'].cast("____"))


#### Complete the list of colunm names for output correlations dataframe
nomin=['Rank', 'Well Being', '___', '___']


#### Calculate the length of input spark dataframe
n_numerical = __(nomin)


#### Iterate through each row column combination calculating the correlations, fill in the input float dataframe and the names list.


corr = []
for i in range(0, n_numerical):
  temp = [None] * i
  for j in range(i,n_numerical):
    temp.append(____.corr(____[i], ____[j]))
  corr.append([nomin[i] + temp)
correlations = spark.createDataFrame(corr, ['Column'] + nomin)
correlations.show()
```


## script.py

```python
df = spark.read.format("csv").option("header","true").load("googledrive.csv")
# convert strings to floats
flts = df.select(df['Rank'].cast("float"),df['Total Score'].cast("float"), df['Well Being'].cast("float),df['Work'].cast("float"),df.['Environment'].cast("float"))

# create list of colunm names for output dataframe
nomin=['Rank', 'Well Being', 'Work', 'Environment']

# calculate the length of input spark dataframe
n_numerical = len(nomin)

# iterate through each row column combination calculating the correlations
corr = []

for i in range(0, n_numerical):
  temp = [None] * i
  for j in range(i,n_numerical):
    temp.append(flts.corr(nomin[i], nomin[j]))
  corr.append([nomin[i] + temp)
correlations = spark.createDataFrame(corr, ['Column'] + nomin)
correlations.show()
```
