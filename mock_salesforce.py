"""
Mock Salesforce Connection - For Testing/Demo
Replace with real Salesforce connection in production
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class MockSalesforceConnection:
    """Mock Salesforce connection with demo data"""
    
    def query_all(self, soql: str) -> Dict[str, Any]:
        """Return mock data based on SOQL query"""
        
        # Orders from yesterday
        if "OrderItem" in soql and "YESTERDAY" in soql:
            return {
                "records": [
                    {
                        "Id": "00uxx000001",
                        "Order": {
                            "Id": "a1Bxxxx000001",
                            "Type": "Change Plan",
                            "Status": "Submitted",
                            "vlocity_cmt__OrderStatus__c": "Submitted",
                            "vlocity_cmt__Reason__c": "Upgrade to Premium Plan",
                            "vlocity_cmt__Notes__c": "Customer requested upgrade",
                            "vlocity_cmt__OrchestrationPlanId__r": {"Id": "a3Cxxxx000001"},
                            "CreatedBy": {"Name": "John Doe"}
                        },
                        "CreatedDate": "2025-11-09T15:45:00Z",
                        "LastModifiedDate": "2025-11-09T15:45:00Z",
                        "PR_MSISDN__c": "12218071145",
                        "vlocity_cmt__BillingAccountId__r": {
                            "PR_Mobile_Billing_Number__c": "23583579-23582472",
                            "Segment__c": "Postpaid",
                            "vlocity_cmt__AccountPaymentType__c": "Direct Debit"
                        }
                    }
                ]
            }
        
        # Assets for MSISDN
        if "Asset" in soql and "12218071145" in soql:
            return {
                "records": [
                    # Mobile Line Asset
                    {
                        "Id": "02ixx0000008XYXAA",
                        "PR_MSISDN__c": "12218071145",
                        "Product2": {
                            "Name": "Mobile Line Unlimited",
                            "ProductCode": "ML-UNLIM",
                            "vlocity_cmt__ParentClassCode__c": "PR_B2C_Mobile_Line_Class"
                        },
                        "vlocity_cmt__ProvisioningStatus__c": "Active",
                        "vlocity_cmt__RootItemId__c": "a01234",
                        "vlocity_cmt__ParentItemId__c": None,
                        "vlocity_cmt__AssetReferenceId__c": "line-asset-abc123",
                        "External_Asset_Reference_Id__c": None,
                        "vlocity_cmt__OneTimeCharge__c": None,
                        "vlocity_cmt__RecurringCharge__c": 39.99,
                        "Disconnection_Reason__c": "N/A",
                        "vlocity_cmt__DisconnectDate__c": None,
                        "CreatedDate": "2025-11-01T10:00:00Z",
                        "LastModifiedDate": "2025-11-09T15:45:00Z",
                        "CreatedBy": {"Name": "Admin User"},
                        "LastModifiedBy": {"Name": "Jane Smith"},
                        "vlocity_cmt__OrderId__r": {"Id": "a1Bxxxx000001"}
                    },
                    # Device Asset
                    {
                        "Id": "02ixx0000008XYXAB",
                        "PR_MSISDN__c": "12218071145",
                        "Product2": {
                            "Name": "iPhone 15",
                            "ProductCode": "DEV-IP15",
                            "vlocity_cmt__ParentClassCode__c": "PR_B2C_Mobile_Device_Class"
                        },
                        "vlocity_cmt__ProvisioningStatus__c": "Active",
                        "vlocity_cmt__RootItemId__c": "dev-root-xyz789",
                        "vlocity_cmt__ParentItemId__c": None,
                        "vlocity_cmt__AssetReferenceId__c": "dev-asset-xyz789",
                        "External_Asset_Reference_Id__c": "line-asset-abc123",
                        "vlocity_cmt__OneTimeCharge__c": 599.99,
                        "vlocity_cmt__RecurringCharge__c": 0,
                        "Disconnection_Reason__c": "N/A",
                        "vlocity_cmt__DisconnectDate__c": None,
                        "CreatedDate": "2025-11-02T12:00:00Z",
                        "LastModifiedDate": "2025-11-09T15:45:00Z",
                        "CreatedBy": {"Name": "Admin User"},
                        "LastModifiedBy": {"Name": "Jane Smith"},
                        "vlocity_cmt__OrderId__r": {"Id": "a1Bxxxx000001"}
                    },
                    # Add-On Asset
                    {
                        "Id": "02ixx0000008XYXAC",
                        "PR_MSISDN__c": "12218071145",
                        "Product2": {
                            "Name": "International Add-On",
                            "ProductCode": "ADDON-INTL",
                            "vlocity_cmt__ParentClassCode__c": "PR_B2C_Mobile_Add_On"
                        },
                        "vlocity_cmt__ProvisioningStatus__c": "Active",
                        "vlocity_cmt__RootItemId__c": "a01234",
                        "vlocity_cmt__ParentItemId__c": "a01234",
                        "vlocity_cmt__AssetReferenceId__c": "addon-asset-123",
                        "External_Asset_Reference_Id__c": None,
                        "vlocity_cmt__OneTimeCharge__c": 0,
                        "vlocity_cmt__RecurringCharge__c": 9.99,
                        "Disconnection_Reason__c": "N/A",
                        "vlocity_cmt__DisconnectDate__c": None,
                        "CreatedDate": "2025-11-02T13:00:00Z",
                        "LastModifiedDate": "2025-11-09T15:45:00Z",
                        "CreatedBy": {"Name": "Admin User"},
                        "LastModifiedBy": {"Name": "Jane Smith"},
                        "vlocity_cmt__OrderId__r": {"Id": "a1Bxxxx000001"}
                    }
                ]
            }
        
        return {"records": []}