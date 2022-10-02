# ETL with Movie Data

## Overview of Project

### Purpose

The analyst was asked by the client to do a complete ETL process using Python and SQL to clean data related to movies and their ratings and upload them to a PostgreSQL database. A combination of Wikipedia, Kaggle, and IMDB data was used, in both CSV and JSON formats. The analyst ultimately delivered a relational database with easy to query information about the pool of movies selected.

### Methods

The code used for ETL may be found [here](https://github.com/cbeckler/movies_etl/blob/main/ETL_create_database.ipynb).

#### Wiki Data--JSON with Regex Cleaning

The Wikipedia data came in as a JSON, and was immediately filtered for movies only by excluding data with nonnull `No. of episodes` variable information, and additionally filtered to contain nonnull `imdb_link` values, since that would later be used for joins. A `clean_movie` function was created that used a for loop and `.pop()` to extract foreign language alternate titles, and combine them all into one variable named `alt_titles`. It also combined the data from columns with different names that contained the same type of information (for example, `Produced by` and `Producer(s)`).

The cleaned wiki data was then transformed into a pandas dataframe, and regex was used to extract the IMDB ID from the `imdb_link` variable. Duplicate IMDB IDs were dropped, and then columns with less than 90% nonnull values were dropped using list comprehension to create a list of valid columns. 

After, regex search parameters were used with nested functions to clean the data in the `box_office` and `budget` variables. The function may be seen below:

```
def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan 
            
# Clean the box office column in the wiki_movies_df DataFrame.
wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
```

Next, a simpler regex operation was able to be performed to clean `release_date` using `str.extract()`, as seen below:

```
# hold nonnull values convert lists to strings
release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

# create regex forms

# month, dd, yyyy
date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]?\d,\s\d{4}'
# yyyy-mm-dd and yyyy/mm/dd
date_form_two = r'\d{4}.[01]\d.[0123]\d'
# month yyyy
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
# yyyy
date_form_four = r'\d{4}'

# extract dates 
release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)

# use pd.to_datetime() to convert, setting infer_datetime_format=True since there are multiple date formats
wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|
                                                                            {date_form_two}|
                                                                            {date_form_three}|
                                                                            {date_form_four})')[0], infer_datetime_format=True)
 ```
 Finally, regex was used to clean the `running_time` data, and then a lambda function was applied to convert any hour + minute times into pure minutes, seen below:
 
 ```
# Clean the running time column in the wiki_movies_df DataFrame.
# var to hold nonnull values convert list to strings
running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

# extract either minute or hour + minute values 
running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

# convert to numeric
## coerce=True to account for empty strings turns them to Nan, then fill with 0
running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

# lambda function to covert hour + minute to minutes if pure minutes = 0
wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
```

#### Kaggle Data

Standard methods of cleaning the Kaggle data CSV were applied, such as filtering for the `video` boolean being True and using `astype()`, `to_numeric()`, and `to_datetime()` to change column types. The data was then joined to the cleaned wiki date with `pd.merge()` using IMDB ID for a new `movies_df`. The Kaggle data was determined to be more comprehensive than the wiki data, so missing wiki values were filled in with Kaggle data when present using a nested function for relevant variables.

#### Ratings Data

Timestamps were converted to datetime format, and then a groupby was used to get the count of each rating score per movie, so that the rating data for each would be converted to a more usable single row format. The data was then joined to the `movies_df` using `pd.merge()` using the Kaggle ID. Null values for rating score counts were then replaced with 0 using `fillna()`.

#### Uploading Cleaned Data to Database

After a connection was established using `create_engine()`, the `movies_df` (without ratings data) was uploaded to the database using `to_sql()` with the `if_exists` method specified as replace. 

The raw ratings data was a much larger file, so first the old data was dropped using a nested function:

```
def drop_table(table_name):
        base = declarative_base()
        metadata = MetaData(engine, reflect=True)
        table = metadata.tables.get(table_name)
        if table is not None:
            base.metadata.drop_all(engine,[table], checkfirst=True)
            print(f"{table} has been dropped.")

drop_table('ratings')
 ```
 
The benefit, but also danger, of this function is that it allows users to drop the table *without* access to the table class. It should be used with considerable caution. Another benefit is that the function will simply pass if the specified table does not already exist. This function was adapted from a solution found [here](https://stackoverflow.com/questions/35918605/how-to-delete-a-table-in-sqlalchemy).

The ratings data was then uploaded in chunks via a for loop, with addtional messages for elapsed time, as seen below:

```
# create a variable for the number of rows imported
rows_imported = 0

# get the start_time from time.time()
start_time = time.time()

for data in pd.read_csv(f"{os.getenv('DSB_PATH')}8_ETL/ratings.csv", chunksize=1000000):

  # print out the range of rows that are being imported
  ## end='' prevents output from going to next line
  print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

  data.to_sql(name='ratings', con=engine, if_exists='append')

  # increment the number of rows imported by the size of 'data'
  rows_imported += len(data)

  # add elapsed time to final print out
  print(f'Done. {time.time() - start_time} total seconds elapsed')
```

The total time to upload the table with this method was approxmiately 30 minutes:

![ratings import time](https://github.com/cbeckler/movies_etl/blob/main/Resources/query%20time.png)

Finally, the `movies_with_ratings_df` was uploaded using the `to_sql()` method with replace option.

## Deliverables

Though not all columns are visible in the screenshot, a `movies` table was created from `movies_df`:

![movies table](https://github.com/cbeckler/movies_etl/blob/main/Resources/movie_data.png)

A  `movies_rated` table was created from `movies_with_ratings_df`. The columns displayed here are the last colums from the movies data and the rating information columns:

![movies_rated table](https://github.com/cbeckler/movies_etl/blob/main/Resources/movies_with_ratings.png)

A `ratings` table was also created from the raw data used for ratings:

![ratings table](https://github.com/cbeckler/movies_etl/blob/main/Resources/ratings.png)




 
