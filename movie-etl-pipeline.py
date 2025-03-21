#!/usr/bin/env python3
"""
Movie ETL Pipeline - Basic Implementation
This script fetches movie data from TMDB API and stores it in a SQLite database.
"""

import os
import time
import json
import logging
import sqlite3
import requests
from dotenv import load_dotenv
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('movie_etl.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('movie_etl')

# Load environment variables
load_dotenv()
API_KEY = os.getenv('TMDB_API_KEY')
if not API_KEY:
    raise ValueError("TMDB_API_KEY not found in environment variables")

# API Configuration
BASE_URL = "https://api.themoviedb.org/3"

class MovieETL:
    """Basic ETL pipeline for movie data from TMDB API."""
    
    def __init__(self, db_path='movie_database.db'):
        """Initialize the ETL pipeline with database path."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """Connect to SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist."""
        try:
            # Movies table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                movie_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                original_title TEXT,
                overview TEXT,
                release_date TEXT,
                budget REAL,
                revenue REAL,
                runtime INTEGER,
                popularity REAL,
                vote_average REAL,
                vote_count INTEGER,
                poster_path TEXT,
                backdrop_path TEXT,
                status TEXT,
                original_language TEXT,
                created_at TEXT
            )
            ''')
            
            # Genres table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS genres (
                genre_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
            ''')
            
            # Movie-Genre relationship table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INTEGER,
                genre_id INTEGER,
                PRIMARY KEY (movie_id, genre_id),
                FOREIGN KEY (movie_id) REFERENCES movies (movie_id),
                FOREIGN KEY (genre_id) REFERENCES genres (genre_id)
            )
            ''')
            
            # Production Companies table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS production_companies (
                company_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                origin_country TEXT
            )
            ''')
            
            # Movie-Production Company relationship table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie_production_companies (
                movie_id INTEGER,
                company_id INTEGER,
                PRIMARY KEY (movie_id, company_id),
                FOREIGN KEY (movie_id) REFERENCES movies (movie_id),
                FOREIGN KEY (company_id) REFERENCES production_companies (company_id)
            )
            ''')
            
            # Cast Members table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS cast_members (
                cast_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                gender INTEGER,
                profile_path TEXT
            )
            ''')
            
            # Movie-Cast relationship table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS movie_cast (
                movie_id INTEGER,
                cast_id INTEGER,
                character TEXT,
                order_position INTEGER,
                PRIMARY KEY (movie_id, cast_id),
                FOREIGN KEY (movie_id) REFERENCES movies (movie_id),
                FOREIGN KEY (cast_id) REFERENCES cast_members (cast_id)
            )
            ''')
            
            self.conn.commit()
            logger.info("Database tables created successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def fetch_popular_movies(self, page=1):
        """Fetch a page of popular movies from TMDB API."""
        try:
            url = f"{BASE_URL}/movie/popular"
            params = {
                "api_key": API_KEY,
                "page": page
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("results", [])
        except requests.RequestException as e:
            logger.error(f"API request error for popular movies page {page}: {e}")
            return []
    
    def fetch_movie_data(self, movie_id):
        """Fetch detailed data for a specific movie."""
        try:
            url = f"{BASE_URL}/movie/{movie_id}"
            params = {
                "api_key": API_KEY,
                "append_to_response": "credits"
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request error for movie {movie_id}: {e}")
            return None
    
    def clean_movie_data(self, movie_data):
        """Clean and transform raw movie data."""
        if not movie_data:
            return None
        
        try:
            # Extract basic movie information
            cleaned_data = {
                "movie": {
                    "movie_id": movie_data.get("id"),
                    "title": movie_data.get("title"),
                    "original_title": movie_data.get("original_title"),
                    "overview": movie_data.get("overview"),
                    "release_date": movie_data.get("release_date"),
                    "budget": movie_data.get("budget"),
                    "revenue": movie_data.get("revenue"),
                    "runtime": movie_data.get("runtime"),
                    "popularity": movie_data.get("popularity"),
                    "vote_average": movie_data.get("vote_average"),
                    "vote_count": movie_data.get("vote_count"),
                    "poster_path": movie_data.get("poster_path"),
                    "backdrop_path": movie_data.get("backdrop_path"),
                    "status": movie_data.get("status"),
                    "original_language": movie_data.get("original_language"),
                    "created_at": datetime.now().isoformat()
                },
                "genres": movie_data.get("genres", []),
                "production_companies": movie_data.get("production_companies", []),
                "cast": movie_data.get("credits", {}).get("cast", [])[:10]  # Top 10 cast members
            }
            
            return cleaned_data
        except Exception as e:
            logger.error(f"Error cleaning movie data for movie {movie_data.get('id')}: {e}")
            return None
    
    def insert_movie_data(self, cleaned_data):
        """Insert cleaned movie data into the database."""
        if not cleaned_data:
            return False
        
        try:
            # Insert movie
            movie = cleaned_data["movie"]
            self.cursor.execute('''
            INSERT OR REPLACE INTO movies (
                movie_id, title, original_title, overview, release_date, budget, revenue,
                runtime, popularity, vote_average, vote_count, poster_path, backdrop_path,
                status, original_language, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                movie["movie_id"], movie["title"], movie["original_title"], movie["overview"],
                movie["release_date"], movie["budget"], movie["revenue"], movie["runtime"],
                movie["popularity"], movie["vote_average"], movie["vote_count"],
                movie["poster_path"], movie["backdrop_path"], movie["status"],
                movie["original_language"], movie["created_at"]
            ))
            
            # Insert genres and movie-genre relationships
            for genre in cleaned_data["genres"]:
                self.cursor.execute('''
                INSERT OR IGNORE INTO genres (genre_id, name)
                VALUES (?, ?)
                ''', (genre["id"], genre["name"]))
                
                self.cursor.execute('''
                INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                VALUES (?, ?)
                ''', (movie["movie_id"], genre["id"]))
            
            # Insert production companies and movie-company relationships
            for company in cleaned_data["production_companies"]:
                self.cursor.execute('''
                INSERT OR IGNORE INTO production_companies (company_id, name, origin_country)
                VALUES (?, ?, ?)
                ''', (company["id"], company["name"], company.get("origin_country")))
                
                self.cursor.execute('''
                INSERT OR IGNORE INTO movie_production_companies (movie_id, company_id)
                VALUES (?, ?)
                ''', (movie["movie_id"], company["id"]))
            
            # Insert cast members and movie-cast relationships
            for i, cast_member in enumerate(cleaned_data["cast"]):
                self.cursor.execute('''
                INSERT OR IGNORE INTO cast_members (cast_id, name, gender, profile_path)
                VALUES (?, ?, ?, ?)
                ''', (
                    cast_member["id"], cast_member["name"], 
                    cast_member.get("gender"), cast_member.get("profile_path")
                ))
                
                self.cursor.execute('''
                INSERT OR IGNORE INTO movie_cast (movie_id, cast_id, character, order_position)
                VALUES (?, ?, ?, ?)
                ''', (
                    movie["movie_id"], cast_member["id"], 
                    cast_member.get("character"), i
                ))
            
            self.conn.commit()
            logger.info(f"Successfully inserted data for movie: {movie['title']} (ID: {movie['movie_id']})")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database error inserting movie {movie['movie_id']}: {e}")
            self.conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Error inserting movie data for movie {movie['movie_id']}: {e}")
            self.conn.rollback()
            return False
    
    def run_pipeline(self, num_pages=5, delay=1):
        """Run the complete ETL pipeline for multiple pages of popular movies."""
        try:
            self.connect_db()
            self.create_tables()
            
            total_processed = 0
            
            for page in range(1, num_pages + 1):
                logger.info(f"Processing popular movies page {page}")
                popular_movies = self.fetch_popular_movies(page)
                
                for basic_movie in popular_movies:
                    movie_id = basic_movie.get("id")
                    
                    # Fetch detailed movie data
                    movie_data = self.fetch_movie_data(movie_id)
                    
                    # Clean the data
                    cleaned_data = self.clean_movie_data(movie_data)
                    
                    # Insert into database
                    if cleaned_data:
                        success = self.insert_movie_data(cleaned_data)
                        if success:
                            total_processed += 1
                    
                    # Respect API rate limits
                    time.sleep(delay)
            
            logger.info(f"ETL pipeline completed. Processed {total_processed} movies")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            if self.conn:
                self.conn.close()
    
    def run_basic_analysis(self):
        """Run basic analysis queries on the collected data."""
        try:
            self.connect_db()
            
            # Genre popularity analysis
            self.cursor.execute('''
            SELECT g.name, COUNT(*) as movie_count, AVG(m.popularity) as avg_popularity
            FROM genres g
            JOIN movie_genres mg ON g.genre_id = mg.genre_id
            JOIN movies m ON mg.movie_id = m.movie_id
            GROUP BY g.name
            ORDER BY avg_popularity DESC
            ''')
            
            genre_popularity = self.cursor.fetchall()
            logger.info("Genre Popularity Analysis:")
            for genre in genre_popularity:
                logger.info(f"{genre[0]}: {genre[1]} movies, Avg Popularity: {genre[2]:.2f}")
            
            # Studio performance analysis
            self.cursor.execute('''
            SELECT pc.name, COUNT(*) as movie_count, AVG(m.revenue) as avg_revenue
            FROM production_companies pc
            JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
            JOIN movies m ON mpc.movie_id = m.movie_id
            WHERE m.revenue > 0
            GROUP BY pc.name
            HAVING movie_count >= 3
            ORDER BY avg_revenue DESC
            LIMIT 10
            ''')
            
            studio_performance = self.cursor.fetchall()
            logger.info("\nTop Studios by Average Revenue:")
            for studio in studio_performance:
                logger.info(f"{studio[0]}: {studio[1]} movies, Avg Revenue: ${studio[2]/1000000:.2f}M")
            
            # Budget efficiency analysis
            self.cursor.execute('''
            SELECT 
                g.name as genre,
                AVG(m.budget) as avg_budget,
                AVG(m.revenue) as avg_revenue,
                AVG(CASE WHEN m.budget > 0 THEN m.revenue / m.budget ELSE NULL END) as avg_roi
            FROM genres g
            JOIN movie_genres mg ON g.genre_id = mg.genre_id
            JOIN movies m ON mg.movie_id = m.movie_id
            WHERE m.budget > 0 AND m.revenue > 0
            GROUP BY g.name
            ORDER BY avg_roi DESC
            ''')
            
            budget_efficiency = self.cursor.fetchall()
            logger.info("\nBudget Efficiency by Genre:")
            for genre in budget_efficiency:
                logger.info(f"{genre[0]}: Avg Budget: ${genre[1]/1000000:.2f}M, Avg Revenue: ${genre[2]/1000000:.2f}M, ROI: {genre[3]:.2f}x")
            
        except sqlite3.Error as e:
            logger.error(f"Database error during analysis: {e}")
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
        finally:
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    etl = MovieETL()
    etl.run_pipeline(num_pages=5)  # Process 5 pages of popular movies
    etl.run_basic_analysis()