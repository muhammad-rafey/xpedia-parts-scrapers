#!/usr/bin/env python3
"""
API Server for Xpedia Parts Scrapers.

This script provides a REST API interface to trigger scrapers remotely.
Uses Python's built-in http.server module instead of Flask.
"""

import os
import sys
import json
import uuid
import threading
import time
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

# Add the project root to Python path to ensure modules can be found
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import scraper modules
from src.scrapers.lkq.runner import start_lkq_scraper, in_memory_jobs

# Store running jobs
running_jobs = {}

# In-memory storage for jobs and products
jobs = {}
products = {}

class ScraperAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the Scraper API."""
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        """Set response headers."""
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_OPTIONS(self):
        """Handle OPTIONS requests for CORS."""
        self._set_headers()
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        
        # Health check endpoint
        if path == '/api/health':
            self._handle_health_check()
        
        # List scrapers endpoint
        elif path == '/api/scrapers':
            self._handle_list_scrapers()
        
        # List jobs endpoint
        elif path == '/api/jobs':
            self._handle_list_jobs()
        
        # Get job status endpoint
        elif path.startswith('/api/jobs/'):
            if '/products' in path:
                # Get products for a specific job
                job_id = path.split('/api/jobs/')[1].split('/products')[0]
                self._handle_get_job_products(job_id)
            else:
                # Get job status
                job_id = path.split('/api/jobs/')[1]
                self._handle_get_job(job_id)
                
        # Debug endpoint to see all stored products
        elif path == '/api/debug/products':
            self._handle_debug_products()
            
        # Debug endpoint to see all stored jobs
        elif path == '/api/debug/jobs':
            self._handle_debug_jobs()
        
        # Unknown endpoint
        else:
            self._handle_not_found()
    
    def do_POST(self):
        """Handle POST requests."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        
        # Start LKQ scraper endpoint
        if path == '/api/scrapers/lkq/start':
            self._handle_start_lkq()
        
        # Unknown endpoint
        else:
            self._handle_not_found()
    
    def _handle_health_check(self):
        """Handle health check endpoint."""
        response = {
            "status": "ok",
            "message": "API server is running",
            "timestamp": datetime.now().isoformat()
        }
        self._send_json_response(response)
    
    def _handle_list_scrapers(self):
        """Handle list scrapers endpoint."""
        response = {
            "scrapers": [
                {
                    "id": "lkq",
                    "name": "LKQ Online",
                    "description": "Scrapes product data from LKQ Online website",
                    "endpoint": "/api/scrapers/lkq/start"
                }
                # Add more scrapers here as they are implemented
            ]
        }
        self._send_json_response(response)
    
    def _handle_list_jobs(self):
        """Handle GET /jobs endpoint to list all jobs."""
        try:
            # Get all jobs from in-memory storage
            jobs_list = []
            for job_id, job in in_memory_jobs.items():
                jobs_list.append({
                    "job_id": job_id,
                    "scraper_name": job.get("scraper_name"),
                    "status": job.get("status"),
                    "total_products": job.get("total_products", 0),
                    "start_time": self._json_serial(job.get("start_time")),
                    "end_time": self._json_serial(job.get("end_time")),
                    "execution_time": job.get("execution_time", 0)
                })
            
            response = {
                "status": "success",
                "jobs": jobs_list
            }
            self._send_json_response(response)
        except Exception as e:
            print(f"Error listing jobs: {e}")
            self._send_json_response({
                "status": "error",
                "message": f"Error listing jobs: {str(e)}"
            }, 500)
    
    def _handle_get_job(self, job_id):
        """Handle GET /job/<job_id> endpoint to get a specific job."""
        try:
            # Check running jobs first
            if job_id in running_jobs:
                self._send_json_response({
                    "status": "success",
                    "job": running_jobs[job_id]
                })
                return
                
            # Then check in-memory jobs
            if job_id in in_memory_jobs:
                job = in_memory_jobs[job_id]
                job_dict = {
                    "job_id": job_id,
                    "scraper_name": job.get("scraper_name"),
                    "status": job.get("status"),
                    "total_products": job.get("total_products", 0),
                    "start_time": self._json_serial(job.get("start_time")),
                    "end_time": self._json_serial(job.get("end_time")),
                    "execution_time": job.get("execution_time", 0)
                }
                
                self._send_json_response({
                    "status": "success",
                    "job": job_dict
                })
                return
                
            # Job not found
            self._send_json_response({
                "status": "error",
                "message": f"Job {job_id} not found"
            }, 404)
        except Exception as e:
            print(f"Error getting job: {e}")
            self._send_json_response({
                "status": "error",
                "message": f"Error getting job: {str(e)}"
            }, 500)
    
    def _handle_get_job_products(self, job_id):
        """Handle GET /job/<job_id>/products endpoint to get products for a job."""
        try:
            # Get products from the scraper's in-memory storage
            from src.scrapers.lkq.scraper import in_memory_products
            job_products = in_memory_products.get(job_id, [])
            
            if job_products:
                response = {
                    "status": "success",
                    "job_id": job_id,
                    "product_count": len(job_products),
                    "products": job_products[:10],  # Limit to 10 products for response size
                    "note": "Only showing first 10 products for performance" if len(job_products) > 10 else ""
                }
            else:
                response = {
                    "status": "success",
                    "message": f"No products found for job {job_id}",
                    "products": []
                }
                
            self._send_json_response(response)
        except Exception as e:
            print(f"Error getting products for job: {e}")
            self._send_json_response({
                "status": "error",
                "message": f"Error getting products for job: {str(e)}"
            }, 500)
    
    def _handle_debug_products(self):
        """Handle GET /debug/products endpoint to list all products (for debugging only)."""
        try:
            # Get products from the scraper's in-memory storage
            from src.scrapers.lkq.scraper import in_memory_products
            
            all_products = []
            for job_id, products_list in in_memory_products.items():
                for product in products_list[:5]:  # Only include first 5 from each job
                    all_products.append({
                        "job_id": job_id,
                        "product_data": product
                    })
            
            response = {
                "status": "success",
                "total_products_in_memory": sum(len(products) for products in in_memory_products.values()),
                "products_preview": all_products[:20],  # Limit to 20 products total
                "note": "Limited product preview for performance"
            }
            self._send_json_response(response)
        except Exception as e:
            print(f"Error debugging products: {e}")
            self._send_json_response({
                "status": "error",
                "message": f"Error debugging products: {str(e)}"
            }, 500)
    
    def _handle_debug_jobs(self):
        """Handle debug jobs endpoint."""
        response = {
            "total_jobs": len(in_memory_jobs),
            "job_ids": list(in_memory_jobs.keys()),
            "jobs": in_memory_jobs
        }
        
        self._send_json_response(response)
    
    def _handle_start_lkq(self):
        """Handle POST /start/lkq endpoint to start LKQ scraper."""
        try:
            # Create a new job
            job_id = str(uuid.uuid4())
            
            # Store job in running_jobs dictionary for status tracking
            running_jobs[job_id] = {
                "job_id": job_id,
                "scraper_name": "lkq",
                "status": "started",
                "start_time": datetime.now(),
                "end_time": None,
                "error": None
            }
            
            # Start scraper in a background thread
            print(f"Starting LKQ scraper as thread with job_id: {job_id}")
            thread = threading.Thread(target=run_lkq_scraper, args=(job_id,))
            thread.daemon = True
            thread.start()
            
            response = {
                "status": "success",
                "message": "LKQ scraper started",
                "job_id": job_id
            }
            self._send_json_response(response)
        except Exception as e:
            print(f"Error starting LKQ scraper: {e}")
            self._send_json_response({
                "status": "error",
                "message": f"Error starting LKQ scraper: {str(e)}"
            }, 500)
    
    def _handle_not_found(self):
        """Handle unknown endpoint."""
        self._send_json_response({
            "status": "error",
            "message": f"Endpoint not found: {self.path}"
        }, 404)
    
    def _send_json_response(self, data, status_code=200):
        """Send JSON response."""
        self._set_headers(status_code)
        self.wfile.write(json.dumps(data, default=self._json_serial).encode('utf-8'))
    
    def _json_serial(self, obj):
        """JSON serializer for objects not serializable by default json code."""
        if isinstance(obj, (datetime)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")


def run_lkq_scraper(job_id, params=None):
    """Run the LKQ scraper with the given parameters."""
    try:
        # Start the scraper
        result_job_id = start_lkq_scraper(job_id)
        
        # Update job status if the job exists in running_jobs
        if result_job_id and job_id in running_jobs:
            running_jobs[job_id]["status"] = "running"
            print(f"Job {job_id} is now running")
        elif job_id in running_jobs:
            # Handle case where start_lkq_scraper failed to start the job
            running_jobs[job_id]["status"] = "failed"
            running_jobs[job_id]["error"] = "Failed to start the scraper"
            running_jobs[job_id]["end_time"] = datetime.now().isoformat()
            print(f"Failed to start scraper for job {job_id}")
        
        return result_job_id is not None
        
    except Exception as e:
        print(f"Error running LKQ scraper: {e}")
        
        # Update job status on error if the job exists in running_jobs
        if job_id in running_jobs:
            running_jobs[job_id]["status"] = "error"
            running_jobs[job_id]["error"] = str(e)
            running_jobs[job_id]["end_time"] = datetime.now().isoformat()
        
        return False


def run_server(port=5000):
    """Run the HTTP server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, ScraperAPIHandler)
    print(f"Starting API server on port {port}...")
    print(f"Available endpoints:")
    print(f"  - GET  /api/health")
    print(f"  - GET  /api/scrapers")
    print(f"  - POST /api/scrapers/lkq/start")
    print(f"  - GET  /api/jobs")
    print(f"  - GET  /api/jobs/<job_id>")
    print(f"  - GET  /api/jobs/<job_id>/products")
    print(f"  - GET  /api/debug/products")
    print(f"  - GET  /api/debug/jobs")
    httpd.serve_forever()


if __name__ == "__main__":
    # Get port from environment or use default
    port = int(os.environ.get("PORT", 5000))
    run_server(port) 