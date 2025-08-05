#!/usr/bin/env python3
"""
Test pyvespa in a threading environment similar to Airflow's triggerer.
This mimics how Airflow runs triggers with asyncio.run_in_executor().
"""

import sys
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor

def vespa_feed_sync():
    """Synchronous Vespa feed function (like our hook's feed_iterable)."""
    
    cert_file = "/Users/radu/.vespa/VESPA_CLOUD_APP_NAME/data-plane-public-cert.pem"
    key_file = "/Users/radu/.vespa/VESPA_CLOUD_APP_NAME/data-plane-private-key.pem"
    vespa_url = "https://VESPA_CLOUD_ENDPOINT"
    
    print(f"[Thread {os.getpid()}] Creating Vespa client...")
    
    from vespa.application import Vespa
    vespa_app = Vespa(
        url=vespa_url,
        cert=cert_file,
        key=key_file,
    )
    
    test_docs = [
        {"id": "test1", "fields": {"title": "Test Document 1"}},
        {"id": "test2", "fields": {"title": "Test Document 2"}},
    ]
    
    print(f"[Thread {os.getpid()}] Calling feed_async_iterable...")
    
    # This is the exact pattern used in the trigger
    vespa_app.feed_async_iterable(
        iter=test_docs,
        schema="doc",  # Adjust this to your schema name
        namespace="default",
        callback=lambda response, doc_id: print(f"[Thread {os.getpid()}] Fed: {doc_id} -> {response.status_code}"),
    )
    
    print(f"[Thread {os.getpid()}] feed_async_iterable completed!")
    return {"success": True, "count": len(test_docs)}

async def test_airflow_pattern():
    """Test the exact pattern used by Airflow triggers."""
    
    print("=== Testing Airflow Threading Pattern ===")
    print(f"Main process: {os.getpid()}")
    
    try:
        # This mimics exactly what the trigger does:
        # loop.run_in_executor(None, _feed)
        loop = asyncio.get_event_loop()
        
        print("Running vespa_feed_sync in executor (thread pool)...")
        result = await loop.run_in_executor(None, vespa_feed_sync)
        
        print(f"✅ SUCCESS: {result}")
        return True
        
    except Exception as e:
        print(f"❌ FAILURE: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_direct_call():
    """Test direct call without threading."""
    print("=== Testing Direct Call (No Threading) ===")
    try:
        result = vespa_feed_sync()
        print(f"✅ SUCCESS: {result}")
        return True
    except Exception as e:
        print(f"❌ FAILURE: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print(f"Python version: {sys.version}")
    print(f"Platform: {sys.platform}")
    
    # Test 1: Direct call
    print("\n" + "="*50)
    direct_success = test_direct_call()
    
    # Test 2: Threaded call (like Airflow)
    print("\n" + "="*50)
    threaded_success = asyncio.run(test_airflow_pattern())
    
    print("\n" + "="*50)
    print("SUMMARY:")
    print(f"  Direct call: {'✅ PASS' if direct_success else '❌ FAIL'}")
    print(f"  Threaded call: {'✅ PASS' if threaded_success else '❌ FAIL'}")
    
    if direct_success and not threaded_success:
        print("\n🎯 CONCLUSION: Threading issue - problem is with asyncio.run_in_executor()")
    elif not direct_success:
        print("\n🎯 CONCLUSION: pyvespa issue - crashes even without threading")
    else:
        print("\n🎯 CONCLUSION: Both work - issue might be Airflow-specific")
    
    sys.exit(0 if (direct_success and threaded_success) else 1)