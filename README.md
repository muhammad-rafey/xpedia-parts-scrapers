# Xpedia Parts Scrapers

A collection of web scrapers for auto parts websites to collect product data and store it in a PostgreSQL database using a custom database access layer.

## Project Structure

```
xpedia-parts-scrapers/
├── config/                   # Configuration files
│   └── config.py             # Central configuration for all scrapers
├── src/                      # Source code directory
│   ├── common/               # Common utilities shared across scrapers
│   │   ├── database/         # Database utilities (SQLAlchemy models)
│   │   │   ├── database.py   # SQLAlchemy database operations
│   │   │   ├── models.py     # SQLAlchemy ORM models
│   │   │   └── session.py    # SQLAlchemy session management
│   │   └── utils/            # General utilities
│   │       └── http.py       # HTTP request utilities
│   └── scrapers/             # Individual website scrapers
│       └── lkq/              # LKQ website scraper
│           ├── runner.py     # Entry point for LKQ scraper
│           └── scraper.py    # LKQ scraper implementation
├── .env                      # Environment variables for database connection
├── api_server.py             # API server for triggering scrapers via REST API
├── create_tables.py          # Script to create database tables using sudo
├── sudo_db.py                # Database operations using sudo commands
├── main.py                   # Main entry point for running scrapers
├── requirements.txt          # Project dependencies
└── README.md                 # Project documentation
```

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up the PostgreSQL database:
   - Create a PostgreSQL database named "xpedia-parts"
   - Run the table creation script:
   ```bash
   python create_tables.py
   ```
   
   Note: The script uses `sudo -u postgres` to create tables, so you'll need sudo access.

## Usage

### Command Line

Run a specific scraper from the command line:

```bash
python main.py lkq
```

### API Server

Start the API server:

```bash
python api_server.py
```

The API server will start on port 5000 by default. You can change the port by setting the `PORT` environment variable.

#### API Endpoints

- `GET /api/health` - Health check endpoint
- `GET /api/scrapers` - List available scrapers
- `POST /api/scrapers/lkq/start` - Start the LKQ scraper
- `GET /api/jobs` - List all running and completed jobs
- `GET /api/jobs/<job_id>` - Get status of a specific job

#### Example API Calls (Postman)

1. Start the LKQ scraper:
   - Method: POST
   - URL: http://localhost:5000/api/scrapers/lkq/start
   - Body: `{}` (empty JSON object)
   - Response:
     ```json
     {
       "status": "success",
       "message": "LKQ scraper started",
       "job_id": "12345678-1234-1234-1234-123456789012"
     }
     ```

2. Check job status:
   - Method: GET
   - URL: http://localhost:5000/api/jobs/12345678-1234-1234-1234-123456789012
   - Response:
     ```json
     {
       "scraper": "lkq",
       "status": "running",
       "start_time": "2023-01-01T12:00:00.000000",
       "params": {}
     }
     ```

## Available Scrapers

- `lkq`: Scrapes product data from LKQ Online website

## Adding New Scrapers

To add a new scraper:

1. Create a new directory under `src/scrapers/`
2. Implement the scraper logic following the project structure
3. Add configuration in `config/config.py`
4. Add the scraper to the CLI options in `main.py`
5. Add the scraper to the API server in `api_server.py`

## Database Schema

The application uses the following database schema:

### Jobs Table
- `job_id`: UUID primary key
- `scraper_name`: Name of the scraper
- `start_time`: When the job started
- `end_time`: When the job ended (nullable)
- `status`: Job status (e.g., "started", "completed")
- `total_products`: Number of products scraped (nullable) 
- `execution_time`: Total execution time in seconds (nullable)

### Products Table
- `product_id`: UUID primary key
- `job_id`: Foreign key to Jobs
- `data`: JSONB data containing the product information
- `scraped_at`: When the product was scraped

## Database Operations

The project supports two methods of database operations:

1. **SQLAlchemy ORM** - The standard approach using SQLAlchemy for database operations.
2. **Sudo-based operations** - An alternative approach that uses direct SQL commands with sudo, which bypasses permission issues when the user doesn't have direct write access to the PostgreSQL database.

The current implementation uses the sudo-based approach for reliability.