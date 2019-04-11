#!/anaconda3/bin/python/=

# JT Mundi
# HW 5
# CS 351

import csv
import json
import sys
import mysql.connector
from mysql.connector import errorcode
from prettytable import from_db_cursor
from tqdm import tqdm

# ANSI colors


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    OKYELLOW = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    CYAN = '\u001b[36;1m'
    BACKYEL = '\u001b[43m'
    BACKRESET = '\u001b[0m'
    WHITE = '\u001b[37;1m'
    BACKMEG = '\u001b[45;1m'

# Connect to the database perfomr queries based on user input if any and insert the data into database


def connectSQL():

    # Check for number of arguments passed in by the user
    argcL = int(len(sys.argv))
    if not (3 <= argcL <= 4):
        print(bcolors.HEADER + "***************Interface***************\n" + bcolors.ENDC +
              bcolors.OKGREEN + "./program username password <query #>\n" + bcolors.ENDC +
              """Where ’program’ is your python script. ’username’ is the name of the user for the database.
’password’ is the password. You can assume that the port and hostname of th edatabase is localhost and 3306.
The last argument is optional. If a number is specified, then print that query. Otherwise, print all queries in the order they are shown here.""")
        exit(1)

    print("Establishing connection to SQL Databse")
    userName = sys.argv[1]  # Username
    userPass = sys.argv[2]  # Userpassword

    # Try connection to the database at localhost
    try:
        cnx = mysql.connector.connect(user=userName, password=userPass,
                                      host='localhost', use_pure=True)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print(
                bcolors.FAIL + "Something is wrong with your user name or password" + bcolors.ENDC)
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(bcolors.FAIL + "Database does not exist" + bcolors.ENDC)
        else:
            print(err)
    else:
        print(bcolors.OKGREEN +
              "Successfully connected to Movies Database" + bcolors.ENDC)

        cursor = cnx.cursor()

        # NOTE: The data is only inserted when the program is ran without query number
        # Data will be parsed and inserted each time when called without query number
        # Example ./movieSQL.py user pass

        if(argcL == 4):
            queryE(int(sys.argv[3]), cnx, cursor)
        else:
            # Create the database schema if it does not exist
            createSchema(cnx, cursor)
            # Insert the data from CSV into database
            insertData(cnx, cursor)

            print(bcolors.OKGREEN +
                  "Successfully parsed all the data into the movies Database" + bcolors.ENDC)

            budgetQ(cnx, cursor)
            usaMoviesQ(cnx, cursor)
            revenueQ(cnx, cursor)
            genreQ(cnx, cursor)
            popQ(cnx, cursor)

            # close the connections
            cursor.close()
            cnx.close()

# Execute queries based on user input


def queryE(x, cnx, cursor):
    if(x == 1):
        budgetQ(cnx, cursor)
    elif(x == 2):
        usaMoviesQ(cnx, cursor)
    elif(x == 3):
        revenueQ(cnx, cursor)
    elif(x == 4):
        genreQ(cnx, cursor)
    elif(x == 5):
        popQ(cnx, cursor)
    else:
        print(bcolors.FAIL + "Invalid query number (Valid Range 1-5)" + bcolors.ENDC)
        exit(1)

# Create the database schema


def createSchema(cnx, cursor):
    # Initialize the database schema
    cursor.execute("DROP DATABASE IF EXISTS movies")
    cnx.commit()
    cursor.execute(
        "CREATE SCHEMA IF NOT EXISTS `movies` DEFAULT CHARACTER SET utf8;")

    cnx.commit()

    # Create Movies Table
    cursor.execute("""CREATE TABLE IF NOT EXISTS
                `movies`.`Movie` (
                `idMovie` INT NOT NULL,
                `Homepage` VARCHAR(1000) NULL,
                `Original_Language` VARCHAR(45) NULL,
                `Original_TItle` VARCHAR(1000) NULL,
                `Overview` VARCHAR(2500) NULL,
                `Populatrity` FLOAT NULL,
                `Release_Date` VARCHAR(100) NULL,
                `Revenue` BIGINT NULL,
                `Budget` INT NULL,
                `Runtime` INT NULL,
                `Status` VARCHAR(100) NULL,
                `Tagline` VARCHAR(1000) NULL,
                `Title` VARCHAR(1000) NULL,
                `Vote_Average` FLOAT NULL,
                `Vote_Count` INT NULL,
                PRIMARY KEY (`idMovie`));""")

    cnx.commit()

    # Create Genre Table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `movies`.`Genre` (`idGenre` INT NOT NULL,`Genre_Name` VARCHAR(45) NULL, PRIMARY KEY (`idGenre`))")

    cursor.execute("""CREATE TABLE IF NOT EXISTS
                        `movies`.`idGenere_Movies` (
                        `Genre_idGenre` INT NOT NULL,
                        `Movie_idMovie` INT NOT NULL,
                        `Genre_Moviescol` VARCHAR(45) NOT NULL,
                        PRIMARY KEY (`Genre_Moviescol`),
                        INDEX `fk_Genre_Movies_Genre_idx` (`Genre_idGenre` ASC) VISIBLE,
                        INDEX `fk_Genre_Movies_Movie1_idx` (`Movie_idMovie` ASC) VISIBLE,
                        CONSTRAINT `fk_Genre_Movies_Genre`
                        FOREIGN KEY (`Genre_idGenre`)
                        REFERENCES `movies`.`Genre` (`idGenre`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        CONSTRAINT `fk_Genre_Movies_Movie1`
                        FOREIGN KEY (`Movie_idMovie`)
                        REFERENCES `movies`.`Movie` (`idMovie`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);""")

    # Create Keywords table
    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Keywords` (
                        `idKeywords` INT NOT NULL,
                        `Keywords_Name` VARCHAR(100) NULL,
                        PRIMARY KEY (`idKeywords`));""")

    cnx.commit()

    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`idKeywords_Movies` (`Keywords_idKeywords` INT NOT NULL,
                        `Movie_idMovie` INT NOT NULL,
                        `Keywords_Moviesc` INT NOT NULL,
                        PRIMARY KEY(`Keywords_Moviesc`),
                        INDEX `fk_Keywords_Movies_Keywords1_idx` (`Keywords_idKeywords` ASC) VISIBLE,
                        INDEX `fk_Keywords_Movies_Movie1_idx` (`Movie_idMovie` ASC) VISIBLE,
                        CONSTRAINT `fk_Keywords_Movies_Keywords1`
                        FOREIGN KEY(`Keywords_idKeywords`)
                        REFERENCES `movies`.`Keywords` (`idKeywords`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        CONSTRAINT `fk_Keywords_Movies_Movie1`
                        FOREIGN KEY(`Movie_idMovie`)
                        REFERENCES `movies`.`Movie` (`idMovie`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);""")

    cnx.commit()

    # Create Production compnaies Table
    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Production_Companies` (`idProduction_Companies` INT NOT NULL,
                        `Production_Company_Name` VARCHAR(100) NULL,
                        PRIMARY KEY(`idProduction_Companies`));""")

    cnx.commit()

    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Production_Companis_Movies` (`Production_Companies_idProduction_Companies` INT NOT NULL,
                        `Movie_idMovie` INT NOT NULL,
                        `idProduction_Companis_Moviescol` INT NOT NULL,
                        INDEX `fk_Production_Companis_Movies_Production_Companies1_idx` (`Production_Companies_idProduction_Companies` ASC) VISIBLE,
                        INDEX `fk_Production_Companis_Movies_Movie1_idx` (`Movie_idMovie` ASC) VISIBLE,
                        PRIMARY KEY(`idProduction_Companis_Moviescol`),
                        CONSTRAINT `fk_Production_Companis_Movies_Production_Companies1`
                        FOREIGN KEY(`Production_Companies_idProduction_Companies`)
                        REFERENCES `movies`.`Production_Companies` (`idProduction_Companies`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        CONSTRAINT `fk_Production_Companis_Movies_Movie1`
                        FOREIGN KEY(`Movie_idMovie`)
                        REFERENCES `movies`.`Movie` (`idMovie`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);""")

    cnx.commit()

    # Production Countries
    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Production_Countries` (`idProduction_Country_ISO` VARCHAR(10) NOT NULL,
                        `Country_Name` VARCHAR(45) NULL,
                        PRIMARY KEY(`idProduction_Country_ISO`));""")

    cnx.commit()

    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Production_Countries_Movies` (`Production_Countries_idProduction_Countries` VARCHAR(10) NOT NULL,
                        `Movie_idMovie` INT NOT NULL,
                        `idProduction_Countries_Movies` INT NOT NULL,
                        INDEX `fk_Production_Countries_Movies_Production_Countries1_idx` (`Production_Countries_idProduction_Countries` ASC) VISIBLE,
                        INDEX `fk_Production_Countries_Movies_Movie1_idx` (`Movie_idMovie` ASC) VISIBLE,
                        PRIMARY KEY(`idProduction_Countries_Movies`),
                        CONSTRAINT `fk_Production_Countries_Movies_Production_Countries1`
                        FOREIGN KEY(`Production_Countries_idProduction_Countries`)
                        REFERENCES `movies`.`Production_Countries` (`idProduction_Country_ISO`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        CONSTRAINT `fk_Production_Countries_Movies_Movie1`
                        FOREIGN KEY(`Movie_idMovie`)
                        REFERENCES `movies`.`Movie` (`idMovie`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);""")

    cnx.commit()

    # Spoken Languagues table
    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Spoken_Languagues` (`idSpoken_Languague_ISO` VARCHAR(10) NOT NULL,
                        `Languague_Name` VARCHAR(45) NULL,
                        PRIMARY KEY(`idSpoken_Languague_ISO`));""")

    cnx.commit()

    cursor.execute("""CREATE TABLE IF NOT EXISTS `movies`.`Spoken_Languagues_Movies` (`Spoken_Languagues_idSpoken_Languagues` VARCHAR(10) NOT NULL,
                        `Movie_idMovie` INT NOT NULL,
                        `idSpoken_Languagues_Movies` INT NOT NULL,
                        INDEX `fk_Spoken_Languagues_Movies_Spoken_Languagues1_idx` (`Spoken_Languagues_idSpoken_Languagues` ASC) VISIBLE,
                        INDEX `fk_Spoken_Languagues_Movies_Movie1_idx` (`Movie_idMovie` ASC) VISIBLE,
                        PRIMARY KEY(`idSpoken_Languagues_Movies`),
                        CONSTRAINT `fk_Spoken_Languagues_Movies_Spoken_Languagues1`
                        FOREIGN KEY(`Spoken_Languagues_idSpoken_Languagues`)
                        REFERENCES `movies`.`Spoken_Languagues` (`idSpoken_Languague_ISO`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION,
                        CONSTRAINT `fk_Spoken_Languagues_Movies_Movie1`
                        FOREIGN KEY(`Movie_idMovie`)
                        REFERENCES `movies`.`Movie` (`idMovie`)
                        ON DELETE NO ACTION
                        ON UPDATE NO ACTION);""")

    cnx.commit()


# Parse the data into database from CSV file

def insertData(cnx, cursor):

    print("Parsing data from Movies CSV and inserting it into Database")

    # To turn of progress bar and not use tqdm set showPbar to 0
    showPbar = 1

    if(showPbar == 1):
        pbar = tqdm(total=4803)
    idGenreRelation = 0
    idKeywordRelation = 0
    idCompanyRelation = 0
    idCountryRelation = 0
    idLangugaeRelation = 0

    # Open the Movie data file
    with open('./tmdb_5000_movies.csv', 'r') as csvfile:

        # Read in a dicitonary file
        movieReader = csv.DictReader(csvfile, delimiter=',')

        movieTableTemp = """INSERT INTO movies.Movie(idMovie, Homepage, Original_Language, Original_TItle, Overview, Populatrity, Release_Date, Revenue,Budget, Runtime, Status, Tagline, Title, Vote_Average, Vote_Count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        # Iterate the columns and insert tuples into dtabase.
        for mRow in movieReader:

            # Updat eprogress bar
            if(showPbar == 1):
                pbar.update(1)

            # Add data into Movie table
            cursor.execute(movieTableTemp, (
                mRow['id'],
                mRow['homepage'],
                mRow['original_language'],
                mRow['original_title'].replace("\'", "''"),
                mRow['overview'].replace("\'", "''"),
                mRow['popularity'],
                mRow['release_date'],
                mRow['revenue'],
                mRow['budget'],
                mRow['runtime'] if mRow['runtime'] != "" else 0,
                mRow['status'],
                mRow['tagline'].replace("\'", "''"),
                mRow['title'].replace("\'", "''"),
                mRow['vote_average'],
                mRow['vote_count']))

            # Commit the changes made to the database
            cnx.commit()

            # Load the unatomic data into Relational tables.
            genreMovies = json.loads(mRow['genres'])
            keywordsMovies = json.loads(mRow['keywords'])
            companyMovies = json.loads(mRow['production_companies'])
            countryMovies = json.loads(mRow['production_countries'])
            languageMovies = json.loads(mRow['spoken_languages'])

            # Set the foregin key checks to 0
            cursor.execute("SET FOREIGN_KEY_CHECKS=0")

            # Parse movie genre data into tables
            for i in range(0, len(genreMovies)):
                idGenreRelation = idGenreRelation + 1

                # Genre relation table with movie id and genre id
                genRel = """INSERT INTO `movies`.`idGenere_Movies`
                            (`Genre_idGenre`,
                            `Movie_idMovie`,
                            `Genre_Moviescol`)
                            VALUES (%s, %s, %s);"""
                cursor.execute(
                    genRel, (genreMovies[i]['id'], mRow['id'], idGenreRelation))

                cnx.commit()

                # Genre table with id and genre
                genreT = """REPLACE INTO `movies`.`Genre` (`idGenre`, `Genre_Name`) VALUES (%s, %s);"""

                cursor.execute(
                    genreT, (genreMovies[i]['id'], genreMovies[i]['name']))

                cnx.commit()

            # Parse movie keywords into tables
            for j in range(0, len(keywordsMovies)):

                idKeywordRelation = idKeywordRelation + 1

                # Keywords relation table with movie id and keyword id
                keyRel = """INSERT INTO `movies`.`idKeywords_Movies`
                            (`Keywords_idKeywords`,
                            `Movie_idMovie`,
                            `Keywords_Moviesc`) VALUES (%s, %s, %s);"""
                cursor.execute(
                    keyRel, (keywordsMovies[j]['id'], mRow['id'], idKeywordRelation))

                cnx.commit()

                # Keywords table with id and keywords
                keyT = """REPLACE INTO `movies`.`Keywords`
                            (`idKeywords`,
                            `Keywords_Name`)
                            VALUES (%s, %s);"""

                cursor.execute(
                    keyT, (keywordsMovies[j]['id'], keywordsMovies[j]['name']))

                cnx.commit()

            # Parse production companies into the dtabase
            for k in range(0, len(companyMovies)):

                idCompanyRelation = idCompanyRelation + 1

                comRel = """INSERT INTO `movies`.`Production_Companis_Movies`
                            (`Production_Companies_idProduction_Companies`,
                            `Movie_idMovie`,
                            `idProduction_Companis_Moviescol`)
                            VALUES (%s, %s, %s);"""

                cursor.execute(
                    comRel, (companyMovies[k]['id'], mRow['id'], idCompanyRelation))

                cnx.commit()

                comT = """REPLACE INTO `movies`.`Production_Companies`
                            (`idProduction_Companies`,
                            `Production_Company_Name`)
                            VALUES(%s, %s);"""

                cursor.execute(
                    comT, (companyMovies[k]['id'], companyMovies[k]['name']))

                cnx.commit()

            # Parse countries into movies database
            for l in range(0, len(countryMovies)):

                idCountryRelation = idCountryRelation + 1

                # insert into country table 🤠
                couT = """REPLACE INTO `movies`.`Production_Countries`
                            (`idProduction_Country_ISO`,
                            `Country_Name`)
                            VALUES(%s, %s);"""

                cursor.execute(
                    couT, (countryMovies[l]['iso_3166_1'], countryMovies[l]['name']))
                cnx.commit()

                # insert into countries relation
                couRel = """INSERT INTO `movies`.`Production_Countries_Movies`
                            (`Production_Countries_idProduction_Countries`,
                            `Movie_idMovie`,
                            `idProduction_Countries_Movies`)
                            VALUES (%s, %s, %s);"""

                cursor.execute(
                    couRel, (countryMovies[l]['iso_3166_1'], mRow['id'], idCountryRelation))

                cnx.commit()

            # Parse languages into movies database
            for m in range(0, len(languageMovies)):

                idLangugaeRelation = idLangugaeRelation + 1

                # Insert into language relation table
                lanRel = """REPLACE INTO `movies`.`Spoken_Languagues_Movies`
                            (`Spoken_Languagues_idSpoken_Languagues`,
                            `Movie_idMovie`,
                            `idSpoken_Languagues_Movies`)
                            VALUES (%s, %s, %s);"""

                cursor.execute(
                    lanRel, (languageMovies[m]['iso_639_1'], mRow['id'], idLangugaeRelation))

                cnx.commit()

                # Insert into languages table
                lanT = """REPLACE INTO `movies`.`Spoken_Languagues`
                            (`idSpoken_Languague_ISO`,
                            `Languague_Name`)
                            VALUES(%s, %s);"""

                cursor.execute(
                    lanT, (languageMovies[m]['iso_639_1'], languageMovies[m]['name']))

                cnx.commit()

            cursor.execute("SET FOREIGN_KEY_CHECKS=0")

    # Close the progress bar
    if(showPbar == 1):
        pbar.close()


# Function to print out query data given cursor an query number
def printQ(cursor, qNum):
    print(bcolors.OKGREEN + bcolors.WHITE +
          "******************************************Query %s******************************************" % qNum + bcolors.ENDC + bcolors.BACKRESET)
    print(bcolors.CYAN)
    print(from_db_cursor(cursor))
    print(bcolors.ENDC)

# Average budget of all movies


def budgetQ(cnx, cursor):

    cursor.execute("""SELECT AVG(budget) FROM movies.Movie""")
    printQ(cursor, 1)


# Show only the movies that were produced in the United States. Your output must include the movie title and the production company name.
def usaMoviesQ(cnx, cursor):

    cursor.execute("""  SELECT DISTINCT(Original_TItle), Production_Company_Name
                        FROM movies.Production_Countries_Movies AS CR
                        INNER JOIN movies.Movie as MM
                        ON CR.Movie_idMovie = MM.idMovie
                        INNER JOIN movies.Production_Companis_Movies as PM
                        ON PM.Movie_idMovie = MM.idMovie
                        INNER JOIN movies.Production_Companies as PC
                        ON PC.idProduction_Companies = PM.Movie_idMovie
                        WHERE CR.Production_Countries_idProduction_Countries LIKE 'US' LIMIT 5;""")
    printQ(cursor, 2)

# Show the top 5 movies that made the most revenue. Output includes the movie title and how much revenue it brought in.


def revenueQ(cnx, cursor):

    cursor.execute("""  SELECT Original_TItle, Revenue
                        FROM movies.Movie as M
                        ORDER BY M.Revenue DESC LIMIT 5;""")
    printQ(cursor, 3)

# What movies have both the genre Science Fiction and Mystery.Output includes the movie title and all genres associated with that genre.


def genreQ(cnx, cursor):

    cursor.execute("""SELECT Title, GROUP_CONCAT(Genre_Name SEPARATOR ', ') 
                        FROM(
                                    SELECT MY.Movie_idMovie
                                    FROM(
                                                SELECT Movie_idMovie
                                                FROM movies.idGenere_Movies AS GSF
                                                WHERE GSF.Genre_idGenre = 9648) AS SF
                                    INNER JOIN movies.idGenere_Movies AS MY 
                                    ON MY.Movie_idMovie = SF.Movie_idMovie
                                    WHERE MY.Genre_idGenre = 878) AS F
                        INNER JOIN movies.Movie as M
                        ON  M.idMovie = F.Movie_idMovie
                        INNER JOIN movies.idGenere_Movies as G
                        ON G.Movie_idMovie = M.idMovie
                        INNER JOIN movies.Genre as FG
                        ON FG.idGenre = G.Genre_idGenre
                        GROUP BY Title LIMIT 5;""")
    printQ(cursor, 4)

# Find the movies that have a popularity greater than the average popularity.
# Your output must include the movie title and their popularity.


def popQ(cnx, cursor):

    cursor.execute("""SELECT 
                        Title, 
                        Populatrity 
                        FROM 
                        movies.Movie 
                        WHERE 
                        Movie.Populatrity > (
                            SELECT 
                            AVG(Movie.Populatrity) 
                            FROM 
                            movies.Movie
                        ) 
                        ORDER BY 
                        Movie.Populatrity DESC 
                        LIMIT 
                        5;
                        """)

    printQ(cursor, 5)


if __name__ == "__main__":

    # Connect with sql
    connectSQL()
    print(bcolors.OKGREEN +
          "Successfully exited program" + bcolors.ENDC)
