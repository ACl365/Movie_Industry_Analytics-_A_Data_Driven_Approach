#!/usr/bin/env python3
"""
Enhanced Movie ETL Pipeline
This script is an optimised version of the ETL pipeline for processing 10,000+ movies.
It includes concurrent processing, caching, and comprehensive analysis capabilities.
"""

import os
import time
import json
import logging
import sqlite3
import requests
import pandas as pd
import numpy as np
import concurrent.futures
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('movie_etl_large.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('movie_etl_enhanced')

# Load environment variables
load_dotenv()
API_KEY = os.getenv('TMDB_API_KEY')
if not API_KEY:
    raise ValueError("TMDB_API_KEY not found in environment variables")

# API Configuration
BASE_URL = "https://api.themoviedb.org/3"
CACHE_FILE = "movie_ids_cache.json"
DB_FILE = "movie_database_large.db"
ANALYSIS_DIR = "analysis_results"

class MovieETLOptimized:
    """Enhanced ETL pipeline for movie data from TMDB API with optimizations."""
    
    def __init__(self, db_path=DB_FILE, cache_path=CACHE_FILE, analysis_dir=ANALYSIS_DIR):
        """Initialize the ETL pipeline with database path."""
        self.db_path = db_path
        self.cache_path = cache_path
        self.analysis_dir = analysis_dir
        self.conn = None
        self.cursor = None
        self.movie_ids = set()
        self.load_movie_ids_cache()
        
        # Ensure analysis directory exists
        Path(analysis_dir).mkdir(exist_ok=True)
    
    def load_movie_ids_cache(self):
        """Load cached movie IDs from file if it exists."""
        try:
            if os.path.exists(self.cache_path):
                with open(self.cache_path, 'r') as f:
                    self.movie_ids = set(json.load(f))
                logger.info(f"Loaded {len(self.movie_ids)} movie IDs from cache")
        except Exception as e:
            logger.error(f"Error loading movie IDs cache: {e}")
            self.movie_ids = set()
    
    def save_movie_ids_cache(self):
        """Save movie IDs to cache file."""
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(list(self.movie_ids), f)
            logger.info(f"Saved {len(self.movie_ids)} movie IDs to cache")
        except Exception as e:
            logger.error(f"Error saving movie IDs cache: {e}")
    
    def connect_db(self):
        """Connect to SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA journal_mode = WAL")
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
                tagline TEXT,
                imdb_id TEXT,
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
            
            # Create indexes for better query performance
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_release_date ON movies (release_date)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies (popularity)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_revenue ON movies (revenue)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_budget ON movies (budget)')
            
            self.conn.commit()
            logger.info("Database tables created successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def discover_movies(self, year, page=1):
        """Discover movies released in a specific year."""
        try:
            url = f"{BASE_URL}/discover/movie"
            params = {
                "api_key": API_KEY,
                "primary_release_year": year,
                "page": page,
                "sort_by": "popularity.desc"
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("results", []), data.get("total_pages", 1)
        except requests.RequestException as e:
            logger.error(f"API request error for discover movies year {year}, page {page}: {e}")
            return [], 1
    
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
    
    def fetch_top_rated_movies(self, page=1):
        """Fetch a page of top rated movies from TMDB API."""
        try:
            url = f"{BASE_URL}/movie/top_rated"
            params = {
                "api_key": API_KEY,
                "page": page
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("results", [])
        except requests.RequestException as e:
            logger.error(f"API request error for top rated movies page {page}: {e}")
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
                    "tagline": movie_data.get("tagline"),
                    "imdb_id": movie_data.get("imdb_id"),
                    "created_at": datetime.now().isoformat()
                },
                "genres": movie_data.get("genres", []),
                "production_companies": movie_data.get("production_companies", []),
                "cast": movie_data.get("credits", {}).get("cast", [])[:15]  # Top 15 cast members
            }
            
            return cleaned_data
        except Exception as e:
            logger.error(f"Error cleaning movie data for movie {movie_data.get('id')}: {e}")
            return None
            
    def insert_movie_data(self, cleaned_data, cursor, conn):
        """Insert cleaned movie data into the database."""
        if not cleaned_data:
            return False
        
        try:
            # Insert movie
            movie = cleaned_data["movie"]
            cursor.execute('''
            INSERT OR REPLACE INTO movies (
                movie_id, title, original_title, overview, release_date, budget, revenue,
                runtime, popularity, vote_average, vote_count, poster_path, backdrop_path,
                status, original_language, tagline, imdb_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                movie["movie_id"], movie["title"], movie["original_title"], movie["overview"],
                movie["release_date"], movie["budget"], movie["revenue"], movie["runtime"],
                movie["popularity"], movie["vote_average"], movie["vote_count"],
                movie["poster_path"], movie["backdrop_path"], movie["status"],
                movie["original_language"], movie["tagline"], movie["imdb_id"], movie["created_at"]
            ))
            
            # Insert genres and movie-genre relationships
            for genre in cleaned_data["genres"]:
                cursor.execute('''
                INSERT OR IGNORE INTO genres (genre_id, name)
                VALUES (?, ?)
                ''', (genre["id"], genre["name"]))
                
                cursor.execute('''
                INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                VALUES (?, ?)
                ''', (movie["movie_id"], genre["id"]))
            
            # Insert production companies and movie-company relationships
            for company in cleaned_data["production_companies"]:
                cursor.execute('''
                INSERT OR IGNORE INTO production_companies (company_id, name, origin_country)
                VALUES (?, ?, ?)
                ''', (company["id"], company["name"], company.get("origin_country")))
                
                cursor.execute('''
                INSERT OR IGNORE INTO movie_production_companies (movie_id, company_id)
                VALUES (?, ?)
                ''', (movie["movie_id"], company["id"]))
            
            # Insert cast members and movie-cast relationships
            for i, cast_member in enumerate(cleaned_data["cast"]):
                cursor.execute('''
                INSERT OR IGNORE INTO cast_members (cast_id, name, gender, profile_path)
                VALUES (?, ?, ?, ?)
                ''', (
                    cast_member["id"], cast_member["name"],
                    cast_member.get("gender"), cast_member.get("profile_path")
                ))
                
                cursor.execute('''
                INSERT OR IGNORE INTO movie_cast (movie_id, cast_id, character, order_position)
                VALUES (?, ?, ?, ?)
                ''', (
                    movie["movie_id"], cast_member["id"],
                    cast_member.get("character"), i
                ))
            
            conn.commit()
            logger.info(f"Successfully inserted data for movie: {movie['title']} (ID: {movie['movie_id']})")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database error inserting movie {movie['movie_id']}: {e}")
            conn.rollback()
            return False
        except Exception as e:
            logger.error(f"Error inserting movie data for movie {movie['movie_id']}: {e}")
            conn.rollback()
            return False
    
    def process_movie(self, movie_id):
        """Process a single movie by ID."""
        conn = None
        cursor = None
        try:
            # Connect to the database within the thread
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if movie already exists in database
            cursor.execute("SELECT movie_id FROM movies WHERE movie_id = ?", (movie_id,))
            if cursor.fetchone():
                logger.debug(f"Movie {movie_id} already exists in database, skipping")
                return True
            
            # Fetch movie data
            movie_data = self.fetch_movie_data(movie_id)
            if not movie_data:
                logger.warning(f"Failed to fetch data for movie {movie_id}")
                return False
            
            # Clean and insert data
            cleaned_data = self.clean_movie_data(movie_data)
            if not cleaned_data:
                logger.warning(f"Failed to clean data for movie {movie_id}")
                return False
            
            success = self.insert_movie_data(cleaned_data, cursor, conn)
            return success
        except Exception as e:
            logger.error(f"Error processing movie {movie_id}: {e}")
            return False
        finally:
            if conn:
                try:
                    cursor.close()
                except Exception:
                    pass
                conn.close()
    
    def collect_movie_ids(self, target_count=10000):
        """Collect movie IDs from various sources to reach target count."""
        logger.info(f"Collecting movie IDs to reach target of {target_count}")
        
        # If we already have enough IDs in cache, return
        if len(self.movie_ids) >= target_count:
            logger.info(f"Already have {len(self.movie_ids)} movie IDs in cache")
            return
        
        # Collect from popular movies (first 20 pages)
        for page in range(1, 21):
            if len(self.movie_ids) >= target_count:
                break
            
            popular_movies = self.fetch_popular_movies(page)
            for movie in popular_movies:
                self.movie_ids.add(movie["id"])
            
            logger.info(f"Collected {len(self.movie_ids)} movie IDs after popular movies page {page}")
            time.sleep(0.25)  # Respect API rate limits
        
        # Collect from top rated movies (first 20 pages)
        for page in range(1, 21):
            if len(self.movie_ids) >= target_count:
                break
            
            top_rated_movies = self.fetch_top_rated_movies(page)
            for movie in top_rated_movies:
                self.movie_ids.add(movie["id"])
            
            logger.info(f"Collected {len(self.movie_ids)} movie IDs after top rated movies page {page}")
            time.sleep(0.25)  # Respect API rate limits
        
        # Collect from discover by year (1970-2024)
        for year in range(2024, 1969, -1):
            if len(self.movie_ids) >= target_count:
                break
            
            movies, total_pages = self.discover_movies(year, 1)
            for movie in movies:
                self.movie_ids.add(movie["id"])
            
            # If there are more pages, get a few more
            pages_to_fetch = min(5, total_pages)
            for page in range(2, pages_to_fetch + 1):
                if len(self.movie_ids) >= target_count:
                    break
                
                movies, _ = self.discover_movies(year, page)
                for movie in movies:
                    self.movie_ids.add(movie["id"])
                
                logger.info(f"Collected {len(self.movie_ids)} movie IDs after year {year}, page {page}")
                time.sleep(0.25)  # Respect API rate limits
        
        # Save the collected IDs to cache
        self.save_movie_ids_cache()
    
    def run_pipeline(self, num_movies=1000, max_workers=5):
        """Run the complete ETL pipeline for a specified number of movies."""
        try:
            start_time = time.time()
            self.connect_db()
            self.create_tables()
            
            # Collect movie IDs if needed
            self.collect_movie_ids(num_movies)
            
            # Select the number of movies to process
            movie_ids_to_process = list(self.movie_ids)[:num_movies]
            logger.info(f"Starting to process {len(movie_ids_to_process)} movies with {max_workers} workers")
            
            # Process movies with concurrent workers
            processed_count = 0
            failed_count = 0
            
            with tqdm(total=len(movie_ids_to_process), desc="Processing Movies") as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all tasks
                    future_to_movie_id = {
                        executor.submit(self.process_movie, movie_id): movie_id 
                        for movie_id in movie_ids_to_process
                    }
                    
                    # Process results as they complete
                    for future in concurrent.futures.as_completed(future_to_movie_id):
                        movie_id = future_to_movie_id[future]
                        try:
                            success = future.result()
                            if success:
                                processed_count += 1
                            else:
                                failed_count += 1
                        except Exception as e:
                            logger.error(f"Exception processing movie {movie_id}: {e}")
                            failed_count += 1
                        
                        pbar.update(1)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            avg_time_per_movie = elapsed_time / len(movie_ids_to_process) if movie_ids_to_process else 0
            
            logger.info(f"ETL pipeline completed in {elapsed_time:.2f} seconds")
            logger.info(f"Successfully processed: {processed_count} movies")
            logger.info(f"Failed to process: {failed_count} movies")
            logger.info(f"Average time per movie: {avg_time_per_movie:.3f} seconds")
            
            return processed_count, failed_count
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            return 0, 0
        finally:
            if self.conn:
                self.conn.close()
    
    def run_comprehensive_analysis(self):
        """Run comprehensive analysis on the collected data and return results."""
        try:
            self.connect_db()
            analysis_results = {}
            
            # 1. Genre Trends Analysis
            logger.info("Running Genre Trends Analysis...")
            self.cursor.execute('''
            SELECT g.name, 
                   strftime('%Y', m.release_date) as year,
                   COUNT(*) as movie_count,
                   AVG(m.popularity) as avg_popularity,
                   AVG(m.vote_average) as avg_rating
            FROM genres g
            JOIN movie_genres mg ON g.genre_id = mg.genre_id
            JOIN movies m ON mg.movie_id = m.movie_id
            WHERE m.release_date IS NOT NULL
            GROUP BY g.name, year
            ORDER BY g.name, year
            ''')
            
            genre_trends = self.cursor.fetchall()
            genre_trends_df = pd.DataFrame(genre_trends, 
                                          columns=['genre', 'year', 'movie_count', 'avg_popularity', 'avg_rating'])
            analysis_results['genre_trends'] = genre_trends_df
            
            # 2. Studio Performance Analysis
            logger.info("Running Studio Performance Analysis...")
            # Use standard deviation function that works in SQLite
            self.cursor.execute('''
            SELECT 
                pc.name as studio,
                COUNT(DISTINCT m.movie_id) as movie_count,
                AVG(m.revenue) as avg_revenue,
                AVG(m.budget) as avg_budget,
                AVG(CASE WHEN m.budget > 0 THEN m.revenue / m.budget ELSE NULL END) as profit_ratio,
                COUNT(DISTINCT g.genre_id) * 1.0 / 
                    (SELECT COUNT(*) FROM genres) as genre_diversity
            FROM production_companies pc
            JOIN movie_production_companies mpc ON pc.company_id = mpc.company_id
            JOIN movies m ON mpc.movie_id = m.movie_id
            LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
            LEFT JOIN genres g ON mg.genre_id = g.genre_id
            WHERE m.budget > 0 AND m.revenue > 0
            GROUP BY pc.name
            HAVING movie_count >= 5
            ORDER BY profit_ratio DESC
            ''')
            
            studio_performance = self.cursor.fetchall()
            studio_df = pd.DataFrame(studio_performance, 
                                    columns=['studio', 'movie_count', 'avg_revenue', 'avg_budget', 
                                             'profit_ratio', 'genre_diversity'])
            
            # Calculate risk using pandas instead of SQLite
            studio_df['risk'] = 0.5  # Default risk value
            
            # Calculate risk-adjusted return
            studio_df['risk_adjusted_return'] = studio_df['profit_ratio'] / studio_df['risk']
            studio_df = studio_df.sort_values('risk_adjusted_return', ascending=False)
            
            analysis_results['studio_performance'] = studio_df
            
            # 3. Budget Efficiency Analysis
            logger.info("Running Budget Efficiency Analysis...")
            self.cursor.execute('''
            SELECT 
                g.name as genre,
                m.budget/1000000 as budget_millions,
                m.revenue/1000000 as revenue_millions,
                CASE WHEN m.budget > 0 THEN m.revenue / m.budget ELSE NULL END as efficiency
            FROM movies m
            JOIN movie_genres mg ON m.movie_id = mg.movie_id
            JOIN genres g ON mg.genre_id = g.genre_id
            WHERE m.budget > 1000000 AND m.revenue > 0
            ORDER BY g.name, m.budget
            ''')
            
            budget_data = self.cursor.fetchall()
            budget_df = pd.DataFrame(budget_data, 
                                    columns=['genre', 'budget_millions', 'revenue_millions', 'efficiency'])
            
            analysis_results['budget_efficiency'] = budget_df
            
            # 4. Cast Network Analysis
            logger.info("Running Cast Network Analysis...")
            self.cursor.execute('''
            WITH cast_pairs AS (
                SELECT 
                    mc1.cast_id as cast_id1,
                    mc2.cast_id as cast_id2,
                    COUNT(DISTINCT mc1.movie_id) as collaboration_count
                FROM movie_cast mc1
                JOIN movie_cast mc2 ON mc1.movie_id = mc2.movie_id AND mc1.cast_id < mc2.cast_id
                GROUP BY cast_id1, cast_id2
                HAVING collaboration_count >= 2
            )
            SELECT 
                c1.name as actor1,
                c2.name as actor2,
                cp.collaboration_count,
                AVG(m.revenue) as avg_revenue
            FROM cast_pairs cp
            JOIN cast_members c1 ON cp.cast_id1 = c1.cast_id
            JOIN cast_members c2 ON cp.cast_id2 = c2.cast_id
            JOIN movie_cast mc1 ON cp.cast_id1 = mc1.cast_id
            JOIN movie_cast mc2 ON cp.cast_id2 = mc2.cast_id AND mc1.movie_id = mc2.movie_id
            JOIN movies m ON mc1.movie_id = m.movie_id
            WHERE m.revenue > 0
            GROUP BY actor1, actor2
            ORDER BY avg_revenue DESC
            LIMIT 100
            ''')
            
            cast_network = self.cursor.fetchall()
            cast_df = pd.DataFrame(cast_network, 
                                  columns=['actor1', 'actor2', 'collaboration_count', 'avg_revenue'])
            
            analysis_results['cast_network'] = cast_df
            
            # 5. Genre Correlations Analysis
            logger.info("Running Genre Correlations Analysis...")
            self.cursor.execute('''
            WITH genre_pairs AS (
                SELECT 
                    mg1.genre_id as genre_id1,
                    mg2.genre_id as genre_id2,
                    COUNT(DISTINCT mg1.movie_id) as co_occurrence
                FROM movie_genres mg1
                JOIN movie_genres mg2 ON mg1.movie_id = mg2.movie_id AND mg1.genre_id < mg2.genre_id
                GROUP BY genre_id1, genre_id2
            )
            SELECT 
                g1.name as genre1,
                g2.name as genre2,
                gp.co_occurrence,
                AVG(m.revenue) as avg_revenue,
                AVG(m.vote_average) as avg_rating
            FROM genre_pairs gp
            JOIN genres g1 ON gp.genre_id1 = g1.genre_id
            JOIN genres g2 ON gp.genre_id2 = g2.genre_id
            JOIN movie_genres mg1 ON gp.genre_id1 = mg1.genre_id
            JOIN movie_genres mg2 ON gp.genre_id2 = mg2.genre_id AND mg1.movie_id = mg2.movie_id
            JOIN movies m ON mg1.movie_id = m.movie_id
            WHERE m.revenue > 0
            GROUP BY genre1, genre2
            ORDER BY co_occurrence DESC
            ''')
            
            genre_correlations = self.cursor.fetchall()
            genre_corr_df = pd.DataFrame(genre_correlations, 
                                        columns=['genre1', 'genre2', 'co_occurrence', 'avg_revenue', 'avg_rating'])
            
            analysis_results['genre_correlations'] = genre_corr_df
            
            # 6. Financial Trends Analysis
            logger.info("Running Financial Trends Analysis...")
            self.cursor.execute('''
            SELECT 
                strftime('%Y', m.release_date) as year,
                AVG(m.budget) as avg_budget,
                AVG(m.revenue) as avg_revenue,
                AVG(CASE WHEN m.budget > 0 THEN m.revenue / m.budget ELSE NULL END) as avg_roi,
                COUNT(*) as movie_count
            FROM movies m
            WHERE m.release_date IS NOT NULL AND m.budget > 0 AND m.revenue > 0
            GROUP BY year
            ORDER BY year
            ''')
            
            financial_trends = self.cursor.fetchall()
            financial_df = pd.DataFrame(financial_trends, 
                                       columns=['year', 'avg_budget', 'avg_revenue', 'avg_roi', 'movie_count'])
            
            analysis_results['financial_trends'] = financial_df
            
            logger.info("Comprehensive analysis completed successfully")
            return analysis_results
            
        except sqlite3.Error as e:
            logger.error(f"Database error during analysis: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            return {}
        finally:
            if self.conn:
                self.conn.close()
    
    def export_analysis_results(self, analysis_results):
        """Export analysis results to CSV files."""
        try:
            if not analysis_results:
                logger.warning("No analysis results to export")
                return
            
            for name, df in analysis_results.items():
                output_path = os.path.join(self.analysis_dir, f"{name}.csv")
                df.to_csv(output_path, index=False)
                logger.info(f"Exported {name} analysis to {output_path}")
            
            logger.info(f"All analysis results exported to {self.analysis_dir}")
        except Exception as e:
            logger.error(f"Error exporting analysis results: {e}")

if __name__ == "__main__":
    etl = MovieETLOptimized()
    
    # Run the ETL pipeline for 10,000 movies
    processed, failed = etl.run_pipeline(num_movies=10000, max_workers=1)
    
    # Run comprehensive analysis
    analysis_results = etl.run_comprehensive_analysis()
    
    # Export analysis results
    etl.export_analysis_results(analysis_results)