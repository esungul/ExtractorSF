#!/usr/bin/env python3
"""
Simple CSV Exporter - Main Script with High-Performance Features
"""

import logging
import argparse
from pathlib import Path
import sys
import os

sys.path.insert(0, str(Path(__file__).parent))

from config_loader import ConfigLoader
from csv_exporter import SimpleCSVExporter

def get_salesforce_connection():
    """Get Salesforce connection with better debugging"""
    try:
        from simple_salesforce import Salesforce
        from dotenv import load_dotenv
        
        load_dotenv()
        
        username = os.environ.get('SF_USERNAME')
        password = os.environ.get('SF_PASSWORD') 
        security_token = os.environ.get('SF_SECURITY_TOKEN')
        domain = os.environ.get('SF_DOMAIN', 'login')
        
        print(f"🔧 Checking Salesforce credentials...")
        print(f"   Username: {username}")
        print(f"   Password: {'*' * len(password) if password else 'None'}")
        print(f"   Security Token: {'*' * len(security_token) if security_token else 'None'}")
        print(f"   Domain: {domain}")
        
        if not all([username, password, security_token]):
            print("❌ Missing Salesforce credentials - using mock connection")
            print("   Please check your .env file or environment variables")
            from mock_salesforce import MockSalesforceConnection
            return MockSalesforceConnection()
        
        print("🔄 Connecting to Salesforce...")
        sf = Salesforce(
            username=username,
            password=password, 
            security_token=security_token,
            domain=domain
        )
        
        # Test the connection
        user_info = sf.query("SELECT Id, Name FROM User LIMIT 1")
        print(f"✅ Connected to Salesforce successfully!")
        print(f"   Instance: {sf.sf_instance}")
        print(f"   Session ID: {sf.session_id[:20]}...")
        
        return sf
        
    except Exception as e:
        print(f"❌ Salesforce connection failed: {e}")
        print("   Falling back to mock connection...")
        from mock_salesforce import MockSalesforceConnection
        return MockSalesforceConnection()

def get_salesforce_connection_factory():
    """Factory function for creating Salesforce connections (thread-safe)"""
    def _create_connection():
        return get_salesforce_connection()
    return _create_connection

def setup_logging(debug=False, verbose=False):
    """Setup logging with proper level handling"""
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    if debug:
        level = logging.DEBUG
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        print("🔍 DEBUG LOGGING ENABLED - All debug messages will be shown")
    elif verbose:
        level = logging.INFO
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        print("📝 VERBOSE LOGGING ENABLED - Info messages will be shown")
    else:
        level = logging.WARNING
        log_format = '%(levelname)s - %(message)s'
    
    # Basic configuration
    logging.basicConfig(
        level=level,
        format=log_format,
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Add file handler for debug mode
    if debug:
        file_handler = logging.FileHandler('debug_export.log', mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple CSV Exporter - High Performance Version")
    parser.add_argument("--msisdn", type=str, help="Single MSISDN to export")
    parser.add_argument("--msisdn-file", type=str, help="File with list of MSISDNs")
    parser.add_argument("--config", type=str, default="config.json", help="Config file path")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging (VERBOSE)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging (INFO level)")
    parser.add_argument("--threads", type=int, default=5, help="Number of threads for parallel processing")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for large datasets")
    
    args = parser.parse_args()
    
    if not args.msisdn and not args.msisdn_file:
        print("Error: Either --msisdn or --msisdn-file must be provided")
        sys.exit(1)
    
    # Setup logging first
    setup_logging(debug=args.debug, verbose=args.verbose)
    
    # Now get logger after setup
    logger = logging.getLogger(__name__)
    
    try:
        print("🚀 Starting CSV Exporter...")
        
        # Load config
        config_loader = ConfigLoader()
        config = config_loader.load(args.config)
        print(f"✅ Config loaded from {args.config}")
        
        # Get Salesforce connection factory
        sf_connection_factory = get_salesforce_connection_factory()
        
        # Create exporter with connection factory
        exporter = SimpleCSVExporter(sf_connection_factory, config)
        print("✅ Exporter created")
        
        # Export data
        if args.msisdn:
            print(f"📞 Processing MSISDN: {args.msisdn}")
            exporter.export_single_msisdn(args.msisdn, write_header=True)
        elif args.msisdn_file:
            print(f"📁 Processing MSISDNs from file: {args.msisdn_file}")
            with open(args.msisdn_file, 'r') as f:
                msisdns = [line.strip() for line in f if line.strip()]
            print(f"📊 Found {len(msisdns)} MSISDNs to process")
            
            # Choose the right export method based on dataset size
            if len(msisdns) > 1000:
                print("🔧 Using large dataset optimized processing")
                exporter.export_large_dataset(msisdns)
            elif len(msisdns) > 10:
                print(f"🔧 Using parallel processing with {args.threads} threads")
                exporter.export_multiple_msisdns_parallel(msisdns, max_workers=args.threads)
            else:
                print("🔧 Using sequential processing")
                exporter.export_multiple_msisdns(msisdns)
        
        print("🎉 Export completed!")
        
    except Exception as e:
        print(f"💥 Export failed: {e}")
        if args.debug:
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)