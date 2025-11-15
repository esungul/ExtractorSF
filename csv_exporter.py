"""
Enhanced CSV Exporter with High-Performance Batch Processing
"""

import logging
import csv
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path
import concurrent.futures
from threading import Lock
from tqdm import tqdm
import threading
from functools import lru_cache

logger = logging.getLogger(__name__)

class ConnectionManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ConnectionManager, cls).__new__(cls)
                cls._instance._connections = {}
            return cls._instance
    
    def get_connection(self, sf_connection_factory):
        thread_id = threading.get_ident()
        if thread_id not in self._connections:
            self._connections[thread_id] = sf_connection_factory()
        return self._connections[thread_id]

class SimpleCSVExporter:
    def __init__(self, sf_connection_factory, config: Dict[str, Any]):
        self.sf_connection_factory = sf_connection_factory
        self.config = config
        self.output_file = config['csv_mapping']['output_file']
        self.write_lock = Lock()
        self.connection_manager = ConnectionManager()
        self.performance_config = config.get('performance', {})
        
    def get_sf_connection(self):
        """Get thread-safe Salesforce connection"""
        return self.connection_manager.get_connection(self.sf_connection_factory)

    def build_soql(self, query_config: Dict[str, Any], params: Dict[str, str]) -> str:
        """Build SOQL query from configuration"""
        # Build SELECT clause
        fields = query_config.get('fields', [])
        fields_str = ', '.join(fields)
        
        # Build FROM clause
        from_table = query_config.get('from_table', 'Asset')
        
        # Start building SOQL
        soql = f"SELECT {fields_str} FROM {from_table}"
        
        # Build WHERE clause
        where_conditions = query_config.get('where_conditions', [])
        if where_conditions:
            formatted_conditions = []
            for condition in where_conditions:
                formatted_condition = condition
                for key, value in params.items():
                    formatted_condition = formatted_condition.replace(f'{{{key}}}', value)
                formatted_conditions.append(formatted_condition)
            
            where_clause = ' AND '.join(formatted_conditions)
            soql += f" WHERE {where_clause}"
        
        # Build ORDER BY clause
        order_by = query_config.get('order_by')
        if order_by:
            soql += f" ORDER BY {order_by}"
        
        # Build LIMIT clause
        limit = query_config.get('limit')
        if limit:
            soql += f" LIMIT {limit}"
        
        return soql

    def execute_query(self, query_name: str, params: Dict[str, str]) -> List[Dict[str, Any]]:
        """Execute a single query if enabled, return records"""
        try:
            query_config = self.config['export_queries'][query_name]
            
            # Check if query is enabled
            if not query_config.get('enabled', True):
                logger.info(f"Skipping disabled query: {query_name}")
                return []
            
            soql = self.build_soql(query_config, params)
            
            logger.info(f"Executing: {query_name}")
            logger.debug(f"SOQL: {soql}")
            
            sf = self.get_sf_connection()
            result = sf.query_all(soql)
            records = result.get('records', [])
            
            logger.info(f"Found {len(records)} records")
            return records
            
        except Exception as e:
            logger.error(f"Error in {query_name}: {e}")
            return []

    def execute_batch_query(self, query_name: str, msisdns: List[str], batch_size: int = 100) -> Dict[str, List[Dict]]:
        """Execute queries in batches to reduce API calls"""
        try:
            query_config = self.config['export_queries'][query_name]
            if not query_config.get('enabled', True):
                return {}
            
            all_results = {}
            
            # Process in batches to avoid query length limits
            for i in range(0, len(msisdns), batch_size):
                batch = msisdns[i:i + batch_size]
                msisdn_list = "', '".join(batch)
                
                # Build SOQL with IN clause
                base_soql = self.build_soql(query_config, {'msisdn': 'DUMMY'})
                soql = base_soql.replace("= '{msisdn}'", f"IN ('{msisdn_list}')")
                
                logger.info(f"Executing batch {i//batch_size + 1} for {query_name} with {len(batch)} MSISDNs")
                
                sf = self.get_sf_connection()
                result = sf.query_all(soql)
                records = result.get('records', [])
                
                # Group results by MSISDN
                for record in records:
                    msisdn = record.get('PR_MSISDN__c')
                    if msisdn:
                        if msisdn not in all_results:
                            all_results[msisdn] = []
                        all_results[msisdn].append(record)
            
            return all_results
            
        except Exception as e:
            logger.error(f"Error in batch query {query_name}: {e}")
            return {}

    def extract_attribute_from_json(self, json_attribute_field: Optional[str], attribute_code: str) -> Optional[str]:
        """
        Extract attribute value from JSON using the proven approach from reference function
        """
        logger.debug(f"=== EXTRACTING ATTRIBUTE {attribute_code} USING PROVEN APPROACH ===")
        
        if not json_attribute_field:
            logger.debug(f"❌ No JSON field provided for attribute {attribute_code}")
            return None
        
        try:
            json_data = json.loads(json_attribute_field)
            logger.debug(f"✅ Successfully parsed JSON")
            logger.debug(f"📊 Top-level keys: {list(json_data.keys())}")
            
            # Look for the attribute in ALL categories using the reference approach
            for category_key, attributes_array in json_data.items():
                if not isinstance(attributes_array, list):
                    continue
                    
                logger.debug(f"🔍 Searching in category: {category_key}")
                
                for attribute in attributes_array:
                    if not isinstance(attribute, dict):
                        continue
                        
                    current_code = attribute.get('attributeuniquecode__c')
                    if current_code == attribute_code:
                        logger.debug(f"✅ Found attribute {attribute_code} in category {category_key}")
                        
                        # Use the EXACT same logic as the reference function
                        runtime_info = attribute.get('attributeRunTimeInfo', {})
                        
                        # For picklist attributes, check selectedItem first (as in your reference)
                        selected_item = runtime_info.get('selectedItem', {})
                        if selected_item:
                            value = selected_item.get('value')
                            if value is not None:
                                logger.debug(f"✅ Extracted from selectedItem.value: {value}")
                                return str(value)
                            
                            display_text = selected_item.get('displayText')
                            if display_text:
                                logger.debug(f"✅ Extracted from selectedItem.displayText: {display_text}")
                                return str(display_text)
                        
                        # Fallback to runtime value (as in your reference function)
                        runtime_value = runtime_info.get('value')
                        if runtime_value is not None:
                            logger.debug(f"✅ Extracted from runtimeInfo.value: {runtime_value}")
                            return str(runtime_value)
                        
                        # Final fallback to top-level value__c
                        top_level_value = attribute.get('value__c')
                        if top_level_value is not None:
                            logger.debug(f"✅ Extracted from value__c: {top_level_value}")
                            return str(top_level_value)
                        
                        logger.debug(f"❌ No value found for attribute {attribute_code}")
                        return None
            
            logger.debug(f"❌ Attribute {attribute_code} not found in any category")
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error for attribute {attribute_code}: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error extracting attribute {attribute_code}: {e}")
            return None

    def extract_json_attributes_simple(self, combined_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simple JSON attribute extraction that searches through all assets and categories
        """
        logger.info("🔍 Starting simple JSON attribute extraction...")
        json_attributes = {}
        
        # Check if JSON attribute extraction is enabled
        json_config = self.config.get('json_attribute_extraction', {})
        if not json_config.get('enabled', True):
            logger.info("❌ JSON attribute extraction is disabled in config")
            return json_attributes
        
        json_field = json_config.get('json_field', 'vlocity_cmt__JSONAttribute__c')
        attributes_config = json_config.get('attributes', [])
        
        logger.info(f"Looking for JSON field: {json_field}")
        logger.info(f"Configured to extract {len(attributes_config)} attributes")
        
        # Sort attributes by priority
        attributes_config.sort(key=lambda x: x.get('extraction_priority', 999))
        
        for attr_config in attributes_config:
            attribute_code = attr_config['attributeuniquecode__c']
            csv_column = attr_config['csv_column']
            asset_types = attr_config.get('asset_types', [])
            product_filter = attr_config.get('product_filter')
            required = attr_config.get('required', False)
            
            logger.info(f"Processing attribute: {attribute_code} -> {csv_column}")
            logger.info(f"Targeting asset types: {asset_types}")
            if product_filter:
                logger.info(f"Product filter: {product_filter}")
            
            # Search through specified asset types
            found = False
            for asset_type in asset_types:
                asset_data = combined_data.get(asset_type)
                
                if asset_data is None:
                    logger.debug(f"❌ No data found for asset type: {asset_type}")
                    continue
                
                logger.debug(f"✅ Found data for asset type: {asset_type}")
                
                if isinstance(asset_data, dict):
                    # Single asset
                    if self._should_extract_from_asset(asset_data, product_filter):
                        value = self._extract_from_single_asset_simple(asset_data, json_field, attribute_code)
                        if value is not None:
                            json_attributes[csv_column] = value
                            found = True
                            logger.info(f"✅ Successfully extracted {attribute_code} from {asset_type}: {value}")
                            break
                
                elif isinstance(asset_data, list):
                    # Array of assets
                    for i, asset in enumerate(asset_data):
                        if isinstance(asset, dict) and self._should_extract_from_asset(asset, product_filter):
                            value = self._extract_from_single_asset_simple(asset, json_field, attribute_code)
                            if value is not None:
                                json_attributes[csv_column] = value
                                found = True
                                logger.info(f"✅ Successfully extracted {attribute_code} from {asset_type}[{i}]: {value}")
                                break
                    
                    if found:
                        break
            
            if not found and required:
                logger.warning(f"⚠️ Required attribute {attribute_code} not found in any specified asset types")
                json_attributes[csv_column] = ""  # Empty string for required but missing attributes
        
        logger.info(f"📊 JSON attribute extraction completed: {len(json_attributes)} attributes found")
        return json_attributes

    def _extract_from_single_asset_simple(self, asset: Dict[str, Any], json_field: str, attribute_code: str) -> Optional[str]:
        """Simple extraction from single asset"""
        json_field_data = asset.get(json_field)
        
        if not json_field_data:
            logger.debug(f"❌ JSON field '{json_field}' not found in asset")
            return None
        
        return self.extract_attribute_from_json(json_field_data, attribute_code)

    def _should_extract_from_asset(self, asset: Dict[str, Any], product_filter: Optional[str]) -> bool:
        """Check if we should extract from this asset based on product filter"""
        if not product_filter:
            return True
        
        product_name = asset.get('Product2', {}).get('Name') if isinstance(asset.get('Product2'), dict) else asset.get('Product2.Name')
        return product_name == product_filter

    def extract_fields(self, combined_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract specific fields from targeted assets only
        """
        logger.info("🔍 Starting targeted field extraction...")
        extracted_fields = {}
        
        field_config = self.config.get('field_extraction', {})
        if not field_config.get('enabled', True):
            logger.info("❌ Field extraction is disabled in config")
            return extracted_fields
        
        extraction_rules = field_config.get('extraction_rules', [])
        
        for rule in extraction_rules:
            asset_type = rule['asset_type']
            fields_to_extract = rule.get('fields', [])
            product_filter = rule.get('product_filter')
            
            logger.info(f"Processing field extraction rule for: {asset_type}")
            if product_filter:
                logger.info(f"Product filter: {product_filter}")
            
            asset_data = combined_data.get(asset_type)
            if not asset_data:
                logger.debug(f"❌ No data found for asset type: {asset_type}")
                continue
            
            if isinstance(asset_data, dict):
                # Single asset
                if self._should_extract_from_asset(asset_data, product_filter):
                    self._extract_fields_from_single_asset(asset_data, fields_to_extract, asset_type, extracted_fields)
            
            elif isinstance(asset_data, list):
                # Array of assets
                for i, asset in enumerate(asset_data):
                    if isinstance(asset, dict) and self._should_extract_from_asset(asset, product_filter):
                        self._extract_fields_from_single_asset(asset, fields_to_extract, f"{asset_type}[{i}]", extracted_fields)
        
        logger.info(f"📊 Field extraction completed: {len(extracted_fields)} fields found")
        return extracted_fields

    def _extract_fields_from_single_asset(self, asset: Dict[str, Any], fields_to_extract: List[Dict], asset_path: str, result: Dict[str, Any]):
        """Extract specific fields from a single asset"""
        for field_config in fields_to_extract:
            source_field = field_config['source_field']
            csv_column = field_config['csv_column']
            required = field_config.get('required', False)
            
            # Extract field value
            value = self.extract_field_value(asset, source_field)
            
            if value is not None:
                result_key = f"{asset_path}.{csv_column}"
                result[result_key] = value
                logger.debug(f"✅ Extracted {source_field} from {asset_path}: {value}")
            elif required:
                logger.warning(f"⚠️ Required field {source_field} not found in {asset_path}")

    
    def extract_field_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """Extract value from nested dictionary using dot notation with array support"""
        try:
            parts = field_path.split('.')
            value = data
            
            for part in parts:
                if value is None:
                    return None
                
                # Handle array indices like "recent_orders.0.Order.Type"
                if part.isdigit() and isinstance(value, list):
                    index = int(part)
                    if 0 <= index < len(value):
                        value = value[index]
                    else:
                        return None
                elif isinstance(value, dict):
                    value = value.get(part)
                else:
                    return None
                    
            return value
            
        except Exception as e:
            logger.debug(f"Error extracting field {field_path}: {e}")
            return None

    def get_msisdn_data(self, msisdn: str) -> Dict[str, Any]:
        """Get all data for a single MSISDN and combine into one row - TARGETED QUERIES ONLY"""
        logger.info(f"Fetching data for MSISDN: {msisdn}")
        
        combined_data = {}
        
        try:
            # Only execute enabled and necessary queries
            enabled_queries = {
                name: config for name, config in self.config['export_queries'].items() 
                if config.get('enabled', True)
            }
            
            logger.info(f"Executing {len(enabled_queries)} enabled queries: {list(enabled_queries.keys())}")
            
            # Step 1: Get main asset data (always needed)
            if 'main_asset_data' in enabled_queries:
                logger.info("Step 1: Getting main asset data...")
                main_asset_data = self.execute_query('main_asset_data', {'msisdn': msisdn})
                main_asset = main_asset_data[0] if main_asset_data else {}
                combined_data['main_asset_data'] = main_asset
                
                # Extract IDs for subsequent queries
                root_item_id = main_asset.get('vlocity_cmt__RootItemId__c', '')
                asset_reference_id = main_asset.get('vlocity_cmt__AssetReferenceId__c', '')
                
                logger.info(f"Root Item ID: {root_item_id}")
                logger.info(f"Asset Reference ID: {asset_reference_id}")
            
            # Step 2: Get device details (if enabled)
            device_asset_id = None
            if 'device_details' in enabled_queries and asset_reference_id:
                logger.info("Step 2: Getting device details...")
                device_data = self.execute_query('device_details', {
                    'msisdn': msisdn,
                    'asset_reference_id': asset_reference_id
                })
                combined_data['device_details'] = device_data[0] if device_data else {}
                
                # Extract device asset ID for device children queries
                if device_data:
                    device_asset_id = device_data[0].get('Id')
                    logger.info(f"Device Asset ID: {device_asset_id}")
            
            # Step 3: Get device children (if enabled and device exists)
            if 'device_children' in enabled_queries and device_asset_id:
                logger.info("Step 3: Getting device children...")
                device_children = self.execute_query('device_children', {
                    'msisdn': msisdn,
                    'device_asset_id': device_asset_id
                })
                combined_data['device_children'] = device_children
                logger.info(f"Found {len(device_children)} device children")
            
            # Step 4: Get specific device children (if enabled and device exists)
            if 'specific_device_children' in enabled_queries and device_asset_id:
                logger.info("Step 4: Getting specific device children...")
                specific_device_children = self.execute_query('specific_device_children', {
                    'msisdn': msisdn,
                    'device_asset_id': device_asset_id
                })
                combined_data['specific_device_children'] = specific_device_children
                logger.info(f"Found {len(specific_device_children)} specific device children")
            
            # Step 5: Get specific line children (if enabled)
            if 'specific_line_children' in enabled_queries and root_item_id:
                logger.info("Step 5: Getting specific line children...")
                line_children = self.execute_query('specific_line_children', {
                    'msisdn': msisdn,
                    'root_item_id': root_item_id
                })
                combined_data['specific_line_children'] = line_children
            
            # Step 6: Get recent orders (if enabled)
            if 'recent_orders' in enabled_queries:
                logger.info("Step 6: Getting recent orders...")
                recent_orders = self.execute_query('recent_orders', {'msisdn': msisdn})
                combined_data['recent_orders'] = recent_orders
            
            logger.info("✓ All enabled queries completed successfully")
            
        except Exception as e:
            logger.error(f"Error in get_msisdn_data: {e}")
            # Return whatever data we have so far
            logger.info("Returning partial data due to error")
        
        return combined_data

    def get_msisdn_data_batch(self, msisdns: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get data for multiple MSISDNs using batch queries"""
        all_data = {msisdn: {} for msisdn in msisdns}
        
        # Execute batch queries in optimal order
        queries_order = [
            'main_asset_data',
            'device_details', 
            'device_children',
            'specific_device_children',
            'specific_line_children',
            'recent_orders'
        ]
        
        batch_size = self.performance_config.get('batch_size', 100)
        
        for query_name in queries_order:
            if query_name in self.config['export_queries'] and self.config['export_queries'][query_name].get('enabled', True):
                logger.info(f"Executing batch query: {query_name}")
                batch_results = self.execute_batch_query(query_name, msisdns, batch_size)
                
                # Merge results into all_data
                for msisdn, records in batch_results.items():
                    if msisdn in all_data:
                        all_data[msisdn][query_name] = records[0] if len(records) == 1 else records
        
        return all_data

    def create_csv_row(self, msisdn: str, combined_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a single CSV row from combined data"""
        row = {}
        
        # Extract standard fields from CSV mapping
        for field_config in self.config['csv_mapping']['fields']:
            csv_column = field_config['csv_column']
            source_path = field_config['source']
            
            # Extract value from combined data
            value = self.extract_field_value(combined_data, source_path)
            
            # Format the value for CSV
            if value is None:
                row[csv_column] = ""
            elif isinstance(value, (dict, list)):
                row[csv_column] = str(value)
            else:
                row[csv_column] = value
        
        # Extract JSON attributes using simple approach
        json_attributes = self.extract_json_attributes_simple(combined_data)
        row.update(json_attributes)
        
        # Extract specific fields
        extracted_fields = self.extract_fields(combined_data)
        
        # Map extracted fields to CSV columns
        for field_config in self.config['csv_mapping']['fields']:
            csv_column = field_config['csv_column']
            source_path = field_config['source']
            
            if source_path.startswith('field_extraction.'):
                for key, value in extracted_fields.items():
                    if key.endswith(f".{csv_column}"):
                        row[csv_column] = value
                        break
        
        return row

    def export_single_msisdn(self, msisdn: str, write_header: bool = False):
        """Export data for a single MSISDN to CSV"""
        # Get all data for this MSISDN
        combined_data = self.get_msisdn_data(msisdn)
        
        # Create CSV row
        row = self.create_csv_row(msisdn, combined_data)
        
        # Write to CSV
        mode = 'w' if write_header else 'a'
        with open(self.output_file, mode, newline='', encoding='utf-8') as csvfile:
            # Get all fieldnames from CSV mapping
            fieldnames = [f['csv_column'] for f in self.config['csv_mapping']['fields']]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            if write_header:
                writer.writeheader()
            
            writer.writerow(row)
        
        logger.info(f"✓ Exported data for {msisdn} to {self.output_file}")

    def export_single_msisdn_thread_safe(self, msisdn: str):
        """Thread-safe version of single MSISDN export"""
        combined_data = self.get_msisdn_data(msisdn)
        row = self.create_csv_row(msisdn, combined_data)
        
        # Thread-safe file writing
        with self.write_lock:
            with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = [f['csv_column'] for f in self.config['csv_mapping']['fields']]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(row)

    def export_multiple_msisdns(self, msisdns: List[str]):
        """Export data for multiple MSISDNs"""
        logger.info(f"Exporting {len(msisdns)} MSISDNs to {self.output_file}")
        
        # Write header for first MSISDN
        if msisdns:
            self.export_single_msisdn(msisdns[0], write_header=True)
        
        # Export remaining MSISDNs
        for msisdn in msisdns[1:]:
            self.export_single_msisdn(msisdn, write_header=False)
        
        logger.info(f"✅ Completed export of {len(msisdns)} MSISDNs")

    def export_multiple_msisdns_parallel(self, msisdns: List[str], max_workers: int = 5):
        """Export MSISDNs in parallel using thread pool"""
        logger.info(f"Exporting {len(msisdns)} MSISDNs using {max_workers} workers")
        
        # Write header
        if msisdns:
            self.export_single_msisdn(msisdns[0], write_header=True)
        
        # Process remaining MSISDNs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_msisdn = {
                executor.submit(self.export_single_msisdn_thread_safe, msisdn): msisdn 
                for msisdn in msisdns[1:]
            }
            
            # Process completed tasks with progress bar
            for future in tqdm(concurrent.futures.as_completed(future_to_msisdn), 
                             total=len(future_to_msisdn), 
                             desc="Exporting MSISDNs"):
                msisdn = future_to_msisdn[future]
                try:
                    future.result()
                    logger.debug(f"✓ Completed {msisdn}")
                except Exception as e:
                    logger.error(f"❌ Failed for {msisdn}: {e}")

    def export_large_dataset(self, msisdns: List[str]):
        """Process very large datasets with memory management"""
        batch_size = self.performance_config.get('memory_batch_size', 1000)
        max_workers = self.performance_config.get('max_workers', 5)
        
        logger.info(f"Processing {len(msisdns)} MSISDNs in batches of {batch_size}")
        
        # Write header
        if msisdns:
            self.export_single_msisdn(msisdns[0], write_header=True)
        
        # Process in memory-managed batches
        for i in range(0, len(msisdns), batch_size):
            batch = msisdns[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: MSISDNs {i} to {i + len(batch) - 1}")
            
            # Use parallel processing for each batch
            if len(batch) > 1:
                # Skip first MSISDN in batch (header already written)
                self.export_multiple_msisdns_parallel([batch[0]] + batch[1:], max_workers)
            else:
                self.export_single_msisdn(batch[0], write_header=False)
            
            logger.info(f"✓ Completed batch {i//batch_size + 1}")

    def write_rows_to_csv(self, rows: List[Dict[str, Any]]):
        """Write multiple rows to CSV efficiently"""
        if not rows:
            return
        
        with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [f['csv_column'] for f in self.config['csv_mapping']['fields']]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            for row in rows:
                writer.writerow(row)

    # Debug methods (unchanged)
    def debug_device_hierarchy(self, msisdn: str):
        """Debug method to show device hierarchy"""
        print(f"=== DEBUG DEVICE HIERARCHY FOR {msisdn} ===")
        
        # Get main asset
        main_data = self.execute_query('main_asset_data', {'msisdn': msisdn})
        if not main_data:
            print("❌ No main asset found")
            return
        
        main_asset = main_data[0]
        asset_reference_id = main_asset.get('vlocity_cmt__AssetReferenceId__c')
        
        # Get device details
        device_data = self.execute_query('device_details', {'asset_reference_id': asset_reference_id})
        if not device_data:
            print("❌ No device found")
            return
        
        device = device_data[0]
        device_asset_id = device.get('Id')
        print(f"📱 Main Device: {device.get('Name')} (ID: {device_asset_id})")
        print(f"   Product: {device.get('Product2', {}).get('Name')}")
        
        # Get device children
        print("\n🔍 Querying device children...")
        device_children = self.execute_query('device_children', {'device_asset_id': device_asset_id})
        print(f"   Found {len(device_children)} device children")
        
        for i, child in enumerate(device_children):
            product_name = child.get('Product2', {}).get('Name') if isinstance(child.get('Product2'), dict) else child.get('Product2.Name', 'Unknown')
            print(f"   [{i}] {product_name} (ID: {child.get('Id')})")
        
        # Get specific device children
        print("\n🔍 Querying specific device children...")
        specific_children = self.execute_query('specific_device_children', {'device_asset_id': device_asset_id})
        print(f"   Found {len(specific_children)} specific device children")
        
        for i, child in enumerate(specific_children):
            product_name = child.get('Product2', {}).get('Name') if isinstance(child.get('Product2'), dict) else child.get('Product2.Name', 'Unknown')
            print(f"   [{i}] {product_name} (ID: {child.get('Id')})")
        
        print("=== DEVICE HIERARCHY DEBUG COMPLETE ===")
        
    
    def debug_recent_orders(self, msisdn: str):
        """Debug recent orders extraction"""
        print(f"=== DEBUG RECENT ORDERS FOR {msisdn} ===")
        
        # Get recent orders directly
        recent_orders = self.execute_query('recent_orders', {'msisdn': msisdn})
        print(f"Found {len(recent_orders)} recent orders")
        
        for i, order_item in enumerate(recent_orders):
            print(f"\n📦 Order Item {i}:")
            print(f"   OrderItem ID: {order_item.get('Id')}")
            print(f"   Order Number: {order_item.get('Order', {}).get('OrderNumber')}")
            print(f"   Order Type: {order_item.get('Order', {}).get('Type')}")
            print(f"   Order Reason: {order_item.get('Order', {}).get('vlocity_cmt__Reason__c')}")
            print(f"   Order Status: {order_item.get('Order', {}).get('vlocity_cmt__OrderStatus__c')}")
            print(f"   Product: {order_item.get('vlocity_cmt__Product2Id__r', {}).get('Name')}")
        
        # Test the field extraction
        print(f"\n🔍 Testing Field Extraction:")
        test_data = {'recent_orders': recent_orders}
        
        # Test Recent_Order_Type
        recent_order_type = self.extract_field_value(test_data, 'recent_orders.0.Order.Type')
        print(f"   Recent_Order_Type (index 0): {recent_order_type}")
        
        # Test Recent_Order_Reason  
        recent_order_reason = self.extract_field_value(test_data, 'recent_orders.0.Order.vlocity_cmt__Reason__c')
        print(f"   Recent_Order_Reason (index 0): {recent_order_reason}")
        
        print("=== RECENT ORDERS DEBUG COMPLETE ===")
        
    def debug_field_extraction_detail(self, combined_data: Dict[str, Any]):
        """Detailed debug of field extraction for recent orders"""
        print(f"=== DETAILED FIELD EXTRACTION DEBUG ===")
        
        recent_orders = combined_data.get('recent_orders', [])
        print(f"Recent orders in combined_data: {len(recent_orders)}")
        
        if recent_orders:
            first_order = recent_orders[0]
            print(f"First order structure: {list(first_order.keys())}")
            
            # Check if 'Order' key exists
            order_data = first_order.get('Order')
            print(f"Order data type: {type(order_data)}")
            if order_data:
                print(f"Order keys: {list(order_data.keys())}")
                print(f"Order.Type: {order_data.get('Type')}")
                print(f"Order.vlocity_cmt__Reason__c: {order_data.get('vlocity_cmt__Reason__c')}")
            else:
                print("❌ No 'Order' key found in first order item")
            
            # Test the extraction step by step
            print(f"\n🔍 Step-by-step extraction:")
            
            # Step 1: Get recent_orders
            step1 = combined_data.get('recent_orders')
            print(f"   Step 1 - recent_orders: {type(step1)}, length: {len(step1) if step1 else 0}")
            
            # Step 2: Get index 0
            if step1 and len(step1) > 0:
                step2 = step1[0]
                print(f"   Step 2 - recent_orders[0]: {type(step2)}, keys: {list(step2.keys())}")
                
                # Step 3: Get Order
                step3 = step2.get('Order')
                print(f"   Step 3 - Order: {type(step3)}, keys: {list(step3.keys()) if step3 else 'None'}")
                
                # Step 4: Get Type
                if step3:
                    step4 = step3.get('Type')
                    print(f"   Step 4 - Type: {step4}")
                    
                    step4_reason = step3.get('vlocity_cmt__Reason__c')
                    print(f"   Step 4 - Reason: {step4_reason}")
        
        print("=== DETAILED DEBUG COMPLETE ===")