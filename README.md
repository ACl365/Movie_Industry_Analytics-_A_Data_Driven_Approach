# Movie Industry Analytics: A Data-Driven Approach

A comprehensive data pipeline and analysis framework for studying patterns in the global film industry using data from The Movie Database (TMDB) API. Provides insights into genre trends, studio performance, budget efficiency, and cast collaboration patterns.

## Project Overview

This project leverages data from The Movie Database (TMDB) API to collect, process, and analyse information on up to 10,000 movies, providing insights into genre trends, studio performance, budget efficiency, and cast collaboration patterns.

## Directory Structure

```
movie_industry_analytics/
├── .env                       # Environment file with TMDB API key
├── movie-etl-pipeline.py      # Basic ETL pipeline
├── enhanced-etl-pipeline.py   # Optimized ETL pipeline for up to 10,000 movies
├── movie_database_large.db    # SQLite database (created when running the ETL pipeline)
├── movie_ids_cache.json       # Cache of movie IDs (created when running the ETL pipeline)
├── movie_etl_large.log        # Log file (created when running the ETL pipeline)
├── README.md                  # This file
│
├── analysis_results/          # Analysis output files
│   ├── genre_trends.csv       # Genre popularity by year
│   ├── studio_performance.csv # Studio performance metrics
│   ├── budget_efficiency.csv  # Budget to revenue efficiency
│   ├── cast_network.csv       # Cast collaboration patterns
│   ├── genre_correlations.csv # Genre co-occurrence analysis
│   └── financial_trends.csv   # Budget and revenue trends
│
├── visualization/             # Visualization components
│   ├── genre-visualization.jsx    # Interactive visualization of genre trends
│   ├── studio-performance.jsx     # Studio performance analysis chart
│   └── budget-efficiency.jsx      # Budget efficiency scatter plot by genre
│
└── documentation/             # Project documentation
    ├── portfolio-document.md  # Portfolio document
    └── project-summary.md     # Executive summary of findings
```

## Getting Started

1. **Set up environment**:
   - Ensure you have Python 3.7+ installed
   - Install required packages: `pip install requests pandas sqlite3 python-dotenv tqdm concurrent.futures`
   - Make sure your TMDB API key is in the `.env` file

2. **Run the ETL pipeline**:
   - For a basic run: `python movie-etl-pipeline.py`
   - For the enhanced version: `python enhanced-etl-pipeline.py`

3. **Explore the analysis results**:
   - CSV files in the `analysis_results` directory contain the processed data
   - Visualizations can be integrated into a React application

## Key Findings

- **Horror** exhibits the strongest seasonality pattern with distinct peaks and valleys
- **Science Fiction** shows the strongest positive trend, gaining 26 popularity points over the decade
- **Paramount** leads in risk-adjusted returns (8.31)
- **Horror films** show highest ROI efficiency (3.29x average) with lower initial investment
- **Genre diversity** correlates positively with risk-adjusted returns (r = 0.68)
- Average budgets increased 3.7x (inflation-adjusted) since 1980

## Documentation

For a comprehensive analysis of the findings, refer to:
- `portfolio-document.md` - Academic analysis of the methodology and results
- `project-summary.md` - Executive summary of key findings and implementation details

## Future Work

Future extensions of this project could include:
- Textual analysis of plot descriptions using transformer-based NLP
- Audience demographic analysis
- Integration with social media sentiment data
- Predictive modeling for box office performance
